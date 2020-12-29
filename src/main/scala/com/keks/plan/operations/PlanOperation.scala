package com.keks.plan.operations

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Project}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}


/**
  * Common operations for spark plan representation
  */
trait PlanOperation {

  /* Invisible space character */
  val INV: String = Character.toString(10240)
  val INV3: String = s"$INV$INV$INV"
  val INV6: String = s"$INV3$INV3"
  val INV9: String = s"$INV6$INV3"

  /* like join, filter... */
  val operationName: String

  /* operation description like filter by 'IS NOT NULL'*/
  val operationText: String

  /**
    * Sometimes column could be without table alias.
    * For example .id instead of ACCOUNT.id.
    * This method recursively goes through plan and finds table's alias by unique col id
    * @param columnId unique col id
    * @param plan spark plan
    * @return Option(TABLE_NAME)
    */
  def findTableAliasForColumn(columnId: String, plan: LogicalPlan): Option[String] = {
    def findTableAliasForColumnExpr(columnId: String, expression: Expression): Option[String] = {
      expression match {
        case attr: AttributeReference => if (attr.exprId.jvmId.toString == columnId) attr.qualifier.headOption else None
        case _ => expression.children.toStream.flatMap(findTableAliasForColumnExpr(columnId, _)).headOption
      }
    }

    plan match {
      case Project(list, child) =>
        list.toStream.flatMap(findTableAliasForColumnExpr(columnId, _))
          .headOption
          .orElse(findTableAliasForColumn(columnId, child))
      case _ =>
        plan.children.toStream.flatMap(findTableAliasForColumn(columnId, _)).headOption
    }
  }

  /**
    * Recursively parse spark expression as pretty text.
    * @param expr expression
    * @param rootPlan spark plan
    * @param isDataSource hint to print source table with useful information
    * @param withoutTableName hint print expression without table alias like 'ACCOUNT.id' instead of 'id'
    * @return expression as pretty string
    */
  def toPrettyExpression(expr: Expression,
                         rootPlan: Option[LogicalPlan] = None,
                         isDataSource: Boolean = false,
                         withoutTableName: Boolean): String = {
    val defaultPretty = (expr: Expression) => toPrettyExpression(expr, rootPlan, isDataSource, withoutTableName)
    expr match {
      case EqualTo(l: Expression, r: Expression) =>
        s"${toPrettyExpression(l, rootPlan, isDataSource, withoutTableName)} == ${toPrettyExpression(r, rootPlan, isDataSource, withoutTableName)}"
      case attr: AttributeReference =>
        if (withoutTableName) {
          attr.name
        } else {
          val tableName: String = attr
            .qualifier
            .headOption
            .getOrElse(rootPlan.flatMap(findTableAliasForColumn(attr.exprId.jvmId.toString, _)).getOrElse(""))
            .toUpperCase
          s"$tableName.${attr.name}"
        }
      case Not(expr) => s"Not[${toPrettyExpression(expr, rootPlan, isDataSource, withoutTableName)}]"
      case Literal(value, _) => Option(value).getOrElse("null").toString.replaceAll("\n", "/n")
      case Alias(lit: Literal, asName) =>
        s"['${lit.value}'] $asName: ${lit.dataType}"
      case Alias(child: Expression, asName) =>
        if (isDataSource)
          s"$asName: ${child.dataType}"
        else
          s"${toPrettyExpression(child, rootPlan, isDataSource, withoutTableName)} as $asName"
      case Cast(child, dataType, _) => s"${toPrettyExpression(child, rootPlan, isDataSource, withoutTableName)} cast[$dataType]"
      case WindowExpression(windowFunction, windowSpec) =>
        val windowFunc = s"${toPrettyExpression(windowFunction, rootPlan, isDataSource, withoutTableName)}"
        val partSpec = s"$INV3 PARTITION BY [${windowSpec.partitionSpec.map(toPrettyExpression(_, rootPlan, isDataSource, withoutTableName)).grouped(4).mkString(s"\n$INV9")}] "
        val orderSpec = s"$INV3 ORDER BY ${windowSpec.orderSpec.map(toPrettyExpression(_, rootPlan, isDataSource, withoutTableName)).grouped(4).mkString(s"\n$INV9")}"
        s"$windowFunc OVER [\n$partSpec \n$orderSpec]"
      case rowNumber: RowNumber => rowNumber.prettyName.toUpperCase
      case SortOrder(child, direction, nullOrdering, _) =>
        toPrettyExpression(child, rootPlan, isDataSource, withoutTableName) + " " + direction.sql + nullOrdering.sql
      case AggregateExpression(aggregateFunction, _, _, _) =>
        toPrettyExpression(aggregateFunction, rootPlan, isDataSource, withoutTableName)
      case Count(children) => s"Count[${children.map(toPrettyExpression(_, rootPlan, isDataSource, withoutTableName)).mkString(", ")}]"
      case SortArray(base, ascendingOrder) =>
        val b = toPrettyExpression(base, rootPlan, isDataSource, withoutTableName)
        val orderStr = toPrettyExpression(ascendingOrder, rootPlan, isDataSource, withoutTableName).toLowerCase
        val order =  if(orderStr == "true") "ASC" else if (orderStr == "DESC") "DESC" else orderStr
        s"SortArray[$b $order]"
      case CollectSet(child, _, _) => s"Collect_Set[${toPrettyExpression(child, rootPlan, isDataSource, withoutTableName)}]"
      case CollectList(child, _, _) => s"Collect_List[${toPrettyExpression(child, rootPlan, isDataSource, withoutTableName)}]"
      case CreateNamedStruct(children) =>
        val cols = children
          .filter(_.isInstanceOf[AttributeReference])
          .map(toPrettyExpression(_, rootPlan, isDataSource, withoutTableName))
          .mkString(",")
        s"STRUCT[$cols]"
      case ConcatWs(children) =>
        val separator = toPrettyExpression(children.head, rootPlan, isDataSource, withoutTableName)
        s"Contact [${children.tail.map(toPrettyExpression(_, rootPlan, isDataSource, withoutTableName)).mkString(", ")}" +
          s"\n$INV3$INV3${INV3}BY '$separator']"
      case CaseWhen(branches, elseValue) =>
        val branchCases = branches.map { case (whenExpr, thenExpr) =>
          s"WHEN ${defaultPretty(whenExpr)} THEN ${defaultPretty(thenExpr)}"
        }.mkString(s"\n$INV3")
        val elseCase = elseValue.map(e => s"\n$INV3 ELSE ${defaultPretty(e)}").getOrElse("")
        s"$branchCases $elseCase"
      case IsNull(child) =>
        s"IS NULL[${defaultPretty(child)}]"
      case IsNotNull(child) =>
        s"IS NOT NULL[${defaultPretty(child)}]"
      case CreateArray(children) =>
        s"CreateArrayOf[${children.map(defaultPretty)}]"
      case And(l: Expression, r: Expression) =>
        s"${toPrettyExpression(l, rootPlan, isDataSource, withoutTableName)} \n${INV3}and " +
          s"${toPrettyExpression(r, rootPlan, isDataSource, withoutTableName)}"
      case Or(l: Expression, r: Expression) =>
        s"${defaultPretty(l)} \n${INV3}and ${defaultPretty(r)}"
      case If(predicate, trueValue, falseValue) =>
        s"If[${defaultPretty(predicate)}]" +
          s"\n$INV3$INV3[${defaultPretty(trueValue)}]" +
          s"\n$INV else" +
          s"\n$INV3$INV3[${defaultPretty(falseValue)}]"
      case GreaterThanOrEqual(l: Expression, r: Expression) =>
        s"${defaultPretty(l)} >= ${defaultPretty(r)}"
      case GreaterThan(l: Expression, r: Expression) =>
        s"${defaultPretty(l)} > ${defaultPretty(r)}"
      case LessThanOrEqual(l: Expression, r: Expression) =>
        s"${defaultPretty(l)} <= ${defaultPretty(r)}"
      case LessThan(l: Expression, r: Expression) =>
        s"${defaultPretty(l)} < ${defaultPretty(r)}"
      case Greatest(children) =>
        s"GreatestOf[${children.map(defaultPretty)}]"
      case Coalesce(children) =>
        s"coalesce[${children.map(defaultPretty)}]"
      case ParseToTimestamp(left, _, _) =>
        s"to_timestamp[${defaultPretty(left)}]"
      case x: UnresolvedAttribute =>
        s"${x.name}"
      case Sum(child) =>
        s"Sum[${defaultPretty(child)}]"
      case In(value, list) =>
        s"${defaultPretty(value)} IN[${list.map(defaultPretty)}]"
      case Explode(child) =>
        s"Explode[${defaultPretty(child)}]"
      case udf: ScalaUDF =>
        s"UDF[${udf.children.map(defaultPretty)}]"
      case GetStructField(child, _, name) =>
        s"${defaultPretty(child)}.${name.getOrElse("")}"
      case Contains(left, right) =>
        s"${defaultPretty(left)} Contains[${defaultPretty(right)}]"
      case RegExpReplace(subject, regexp, rep) =>
        s"RegexpReplace[Col=${defaultPretty(subject)},pattern=${defaultPretty(regexp)},value=`${defaultPretty(rep)}`]"
      case StartsWith(left, right) =>
        s"${defaultPretty(left)} StartsWith[${defaultPretty(right)}]"
      case d: DenseRank =>
        d.prettyName
      case First(child, _) =>
        s"First[${defaultPretty(child)}]"
      case Last(child, _) =>
        s"Last[${defaultPretty(child)}]"
      case Upper(child) =>
        s"Upper[${defaultPretty(child)}]"
      case x: DateFormatClass =>
        s"DateFormat[col=${defaultPretty(x.left)},pattern=${defaultPretty(x.right)}${x.timeZoneId.map(e => s",zone=$e").getOrElse("")}]"
      case Min(child) =>
        s"Min[${defaultPretty(child)}]"
      case Max(child) =>
        s"Max[${defaultPretty(child)}]"
      case Like(left, right) =>
        s"${defaultPretty(left)} Like[${defaultPretty(right)}]"
      case x: ParseUrl =>
        s"${x.sql}"
      case Divide(left, right) =>
        s"${defaultPretty(left)} / ${defaultPretty(right)}"
      case DateDiff(endDate, startDate) =>
        s"DateDiff[endDate=${defaultPretty(endDate)}, startDate=${defaultPretty(startDate)}]"
      case DateAdd(startDate, days) =>
        s"DateAdd[date=${defaultPretty(startDate)}, daysToAdd=${defaultPretty(days)}]"
      case Lower(child) =>
        s"Lower[${defaultPretty(child)}]"
      case UnresolvedAlias(child, _) =>
        defaultPretty(child)
      case ArrayContains(left, right) =>
        s"${defaultPretty(left)} ArrayContains[${defaultPretty(right)}]"
      case Subtract(left, right) =>
        s"${defaultPretty(left)} - ${defaultPretty(right)}"
      case Add(left, right) =>
        s"${defaultPretty(left)} + ${defaultPretty(right)}"
      case Multiply(left, right) =>
        s"${defaultPretty(left)} * ${defaultPretty(right)}"
      case Round(child, scale) =>
        s"Round[${defaultPretty(child)}, scale=${defaultPretty(scale)}]"
      case Concat(children) =>
        s"Concat[${children.map(defaultPretty).mkString(", ")}]"
      case MonthsBetween(date1, date2, roundOff, _) =>
        s"MonthsBetween[date1=${defaultPretty(date1)}, date2=${defaultPretty(date2)}, roundOff=${defaultPretty(roundOff)}]"
    }
  }

  /**
    * Detecting data source like CSV, JSON or locally generated table like (Seq(...).toDF(...))
    * @param plan spark plan
    * @return Option(SOURCE_FORMAT, Option(Seq(path)))
    */
  def detectDataSource(plan: LogicalPlan): Option[(String, Seq[String])] = plan match {
    case child: Project => detectDataSource(child.child)
    case _: LocalRelation => Some("GENERATED TABLE", Seq.empty)
    case _: LogicalRDD => Some("GENERATED TABLE", Seq.empty)
    case LogicalRelation(relation, _, _, _) =>
      relation match {
        case HadoopFsRelation(location, _, _, _, fileFormat, _) =>
          val formatStr: String = fileFormat match {
            case jsonFileFormat: JsonFileFormat => jsonFileFormat.shortName
            case csvFileFormat: CSVFileFormat => csvFileFormat.shortName
            case parquetFileFormat: ParquetFileFormat => parquetFileFormat.shortName
          }
          Some(formatStr, location.rootPaths.map(_.toString))
      }
    case _ => None
  }

  implicit class StringOps(seq: Seq[String]) {

    /**
      * In case of multiply columns a good decision is to group them in several strings
      * For instance 'a,b,c,d' is better to print as
      * 'a,b'
      * 'c,d'
      * @param size group size
      */
    def groupedBy(size: Int): Seq[String] = {
      seq.grouped(size).map(_.mkString(",")).toSeq
    }
  }

}