package org.example

import org.apache.spark.sql.SparkSession


object T_5 {

  def ej1(spark: SparkSession): Unit = {

    // In Scala
    // Create cubed function
    val cubed = (s: Long) => {
      s * s * s
    }
    // Register UDF
    spark.udf.register("cubed", cubed)
    // Create temporary view
    spark.range(1, 9).createOrReplaceTempView("udf_test")



    spark.sql("""SELECT id, cubed(id) AS id_cubed FROM udf_test""").show()

    spark.sql("""CREATE TABLE test1 (s STRING) USING parquet""")

    spark.sql("""SELECT s FROM test1 WHERE s IS NOT NULL AND length(s) > 1""")



    def ej2(spark: SparkSession): Unit = {
      
    }
  }
}
