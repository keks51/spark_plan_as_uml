package com.keks.plan.operations.sources.utils

import org.apache.spark.sql.types.DataType

case class SourceColumn(expId: Long,
                        name: String,
                        dataType: DataType) {

  def printNameAndDataType: String = s"$name: $dataType"

}
