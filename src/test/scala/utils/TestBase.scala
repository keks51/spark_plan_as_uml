package utils

import com.google.common.io.Files
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import utils.TestBase.deleteRecursively

import java.io.{File, IOException}


class TestBase extends AnyFlatSpec with GivenWhenThen with Matchers {
  implicit lazy val spark: SparkSession = TestBase.initSpark

  def getResourcePath(resourcePath: String): String = {
    getClass.getResource(resourcePath).getPath
  }

  def withTempDir(f: File => Unit): Unit = {
    val dir: File = Files.createTempDir()
    dir.mkdir()
    try f(dir) finally deleteRecursively(dir)
  }

  def printPlan(plan: LogicalPlan): String = {
    val lines = plan.toString.split("\n")
    val lastLine = lines.length - 1
    lines.zipWithIndex.map { case (line, i) =>
      val strLine = i match {
        case 0 => "/* " + line
        case i if i == lastLine => "   " + line + " */"
        case _ => "   " + line
      }
      strLine
    }.mkString("\n")
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

  implicit class RichBoolean(bool: Boolean) {
    def toOption: Option[Boolean] = if (bool) Option(true) else None
  }

  /**
   * This function deletes a file or a directory with everything that's in it.
   */
  // scalastyle:off null
  private def deleteRecursively(file: File): Unit = {
    if (file != null) {
      try {
        if (file.isDirectory) {
          var savedIOException: IOException = null
          for (child <- listFiles(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  def listFiles(dir: String): Seq[File] = {
    listFiles(new File(dir))
  }

  def listFiles(dir: File): Seq[File] = {
    dir.exists.toOption.map(_ => Option(dir.listFiles()).map(_.toSeq).getOrElse {
      throw new IOException(s"Failed to list files for dir: $dir")
    }).getOrElse(Seq.empty[File])
  }
}
