package io.github.yuokada.presto.udf.scalar

import com.facebook.presto.spi.`type`.StandardTypes
import com.facebook.presto.spi.function.{Description, ScalarFunction, SqlNullable, SqlType}
import io.airlift.slice.Slice
import io.airlift.slice.Slices.utf8Slice

object HelloWorldScalaUDF {

  @Description("Hello World(UDF Practice)")
  @ScalarFunction("hello_worlds")
  @SqlType(StandardTypes.VARCHAR)
  def helloworld(@SqlNullable @SqlType(StandardTypes.VARCHAR) name: Slice): Slice =
    if (name == null || name.toStringUtf8.isEmpty) {
      utf8Slice("Hello World from scala")
    } else {
      utf8Slice(String.format("Hello %s from scala", name.toStringUtf8))
    }

}
