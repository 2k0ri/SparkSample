package org.k2i

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.databricks.spark.avro._

/**
  * Hello world!
  *
  */
object SparkSampleApp {

  case class Person(name: String, favorite_color: String, favorite_numbers: Array[Integer])

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSampleApp")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val avroFile = "/usr/local/opt/apache-spark/libexec/examples/src/main/resources/users.avro"
    val df = sqlContext.read.avro(avroFile)

    val ds = df.as[Person]

    ds.filter(_.favorite_numbers.length > 1).show

  }
}
