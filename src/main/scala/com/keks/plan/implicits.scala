package com.keks.plan

import com.keks.plan.builder.{DiagramBuilder, JsonDiagramBuilder, PlanUmlDiagramBuilder}
import com.keks.plan.parser.{CustomTransformationPlan, CustomTransformationPlanBuilder, DefaultExpressionParser, SparkLogicalRelationParser}
import com.keks.plan.write.{LocalFilePlanSaver, PlanSaver, UmlPlanSaver}
import com.keks.plan.write.types.{JsonPlanType, PlanType}
import org.apache.spark.sql.Dataset


object implicits {

  /**
   * Pretty print methods
   */
  implicit class ParserImp[T](x: Dataset[T]) {

    def printPlan[S <: PlanType](entityName: String,
                                 reportDescription: String,
                                 savePath: String,
                                 planParser: SparkLogicalRelationParser = new SparkLogicalRelationParser(new DefaultExpressionParser()),
                                 builder: DiagramBuilder[S],
                                 saver: PlanSaver[S]): Unit = {
      val customDiagram: CustomTransformationPlan =
        CustomTransformationPlanBuilder.build(x.queryExecution.analyzed, planParser = planParser)

      val data: S = builder.build(savePath = savePath,
        entityName = entityName,
        reportDescription = reportDescription,
        planNodes = customDiagram.nodesList,
        edges = customDiagram.edgesList)
      saver.save(data, savePath, entityName)
    }

    def printAsUml(entityName: String,
                   reportDescription: String,
                   savePath: String,
                   planParser: SparkLogicalRelationParser = new SparkLogicalRelationParser(new DefaultExpressionParser())): Unit = {
      val customDiagram: CustomTransformationPlan =
        CustomTransformationPlanBuilder.build(x.queryExecution.analyzed, planParser = planParser)

      val planUmlData = new PlanUmlDiagramBuilder().build(savePath = savePath,
        entityName = entityName,
        reportDescription = reportDescription,
        planNodes = customDiagram.nodesList,
        edges = customDiagram.edgesList)

      new UmlPlanSaver().save(planUmlData, savePath, entityName)
    }

    def printAsJson(entityName: String,
                    reportDescription: String,
                    savePath: String,
                    planParser: SparkLogicalRelationParser = new SparkLogicalRelationParser(new DefaultExpressionParser())): Unit = {
      val customDiagram: CustomTransformationPlan =
        CustomTransformationPlanBuilder.build(x.queryExecution.analyzed, planParser = planParser)


      val data: JsonPlanType = new JsonDiagramBuilder().build(savePath = savePath,
        entityName = entityName,
        reportDescription = reportDescription,
        planNodes = customDiagram.nodesList,
        edges = customDiagram.edgesList)
      new LocalFilePlanSaver().save(data, savePath, entityName)
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
