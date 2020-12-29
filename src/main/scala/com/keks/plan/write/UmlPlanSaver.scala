package com.keks.plan.write

import com.keks.plan.write.types.PlantUmlPlanType
import net.sourceforge.plantuml.SourceStringReader

import java.io.File


class UmlPlanSaver extends PlanSaver[PlantUmlPlanType] {

  private val PNG_EXTENSION = ".png"

  override def save(data: PlantUmlPlanType, path: String, fileName: String): Unit = {
    val outputPath = new File(path)
    outputPath.mkdirs()
    val diagramFile = new File(s"$outputPath/$fileName$PNG_EXTENSION")
    val reader = new SourceStringReader(data.asString)
    Option(reader.generateImage(diagramFile))
      .orElse(throw new Exception(s"Cannot generate plant UML diagram in path $path"))
  }

}
