package com.keks

import com.keks.plan.parser.{DefaultExpressionParser, ExpressionParser}
import org.apache.spark.sql.Dataset


package object plan {

  /* Invisible space character */
  val INV: String = Character.toString(10240)
  val INV3: String = s"$INV$INV$INV"
  val INV6: String = s"$INV3$INV3"
  val INV9: String = s"$INV6$INV3"

  /**
    * Pretty print methods
    */
  implicit class ParserImp[T](x: Dataset[T]) {

    def printPlanAsUml(entityName: String,
                       reportDescription: String,
                       savePath: String,
                       parser: ExpressionParser = new DefaultExpressionParser,
                       pngLimitSize: Int = 12288): Unit = {
      SparkUmlDiagram.buildAndSaveReport(entityName = entityName,
                                         reportDescription = reportDescription,
                                         pngLimitSize = pngLimitSize,
                                         savePath = savePath,
                                         plan = x.queryExecution.logical,
                                         parser = parser)
    }

  }

  implicit class StringOps(seq: Seq[String]) {

    /**
      * In case of multiply columns a good decision is to group them in several strings
      * For instance 'a,b,c,d' is better to print as
      * 'a,b'
      * 'c,d'
      *
      * @param size group size
      */
    def groupedBy(size: Int): Seq[String] = {
      seq.grouped(size).map(_.mkString(",")).toSeq
    }
  }

}
