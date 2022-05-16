package com.keks.plan

import org.apache.spark.sql.catalyst.ScalaReflection.mirror
import scala.reflect.runtime.universe._
import scala.reflect.api


object Utils {

  def stringClassNameToTypeTag[A](name: String): TypeTag[A] = {
    val sym = mirror.staticClass(name)
    val tpe = sym.selfType
    TypeTag(mirror, new api.TypeCreator {
      def apply[U <: api.Universe with Singleton](m: api.Mirror[U]): U#Type =
        if (m eq mirror) tpe.asInstanceOf[U # Type]
        else throw new IllegalArgumentException(s"Type tag defined in $mirror cannot be migrated to other mirrors.")
    })
  }

}
