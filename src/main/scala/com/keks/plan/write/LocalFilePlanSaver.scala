package com.keks.plan.write

import com.keks.plan.write.types.StringPlanType

import java.io.{BufferedWriter, File}


class LocalFilePlanSaver[T<: StringPlanType] extends PlanSaver[T] {

  override def save(data: T, path: String, fileName: String): Unit = {
    val outputPath = new File(path)
    outputPath.mkdirs()
    val diagramFile = new File(s"$outputPath/$fileName.json")
    val bw = new BufferedWriter(new java.io.FileWriter(diagramFile))
    bw.write(data.asString)
    bw.flush()
    bw.close()
  }

}
