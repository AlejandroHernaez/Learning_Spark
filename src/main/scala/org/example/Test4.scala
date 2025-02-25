package org.example

import org.apache.spark.sql.SparkSession

object Test4 {
  def ej1(spark: SparkSession, csvFile: String): Unit = {



    // Read and create a temporary view
    // Infer schema (note that for larger files you may want to specify the schema)

    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    val df = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(csvFile)
    // Create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")



  }
