package utils

import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class TestBase extends AnyFlatSpec with GivenWhenThen with Matchers {
  implicit lazy val spark: SparkSession = TestBase.initSpark

  def getResourcePath(resourcePath: String): String = {
    getClass.getResource(resourcePath).getPath
  }

}

object TestBase {
  lazy val initSpark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "10")
      .config("spark.port.maxRetries", "16")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "20")
      .appName("Transformations tests")
      .getOrCreate()
  }
}
