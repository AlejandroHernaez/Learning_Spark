package org.example

import org.apache.spark.sql.SparkSession // Define el paquete donde se encuentra este objeto



object T_4 {

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



   /* spark.sql(
      """SELECT distance, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE distance > 1000
        |ORDER BY distance DESC""".stripMargin).show(10)

    spark.sql(
      """SELECT date, distance, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE delay>=120 AND origin = 'SFO' AND DESTINATION = 'ORD'
        |ORDER BY delay DESC""".stripMargin).show(10)

    spark.sql(
      """SELECT
        |substring(date, 0, 2) as month,
        |substring(date, 2 ,2) as day,
        |distance, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE delay>=120 AND origin = 'SFO' AND DESTINATION = 'ORD'
        |ORDER BY delay DESC""".stripMargin).show(10)

    spark.sql(
      """SELECT
        |delay, origin, destination,
        |CASE
          |WHEN delay > 360 THEN 'Very Long Delay'
          |WHEN delay < 360 AND delay > 120 THEN 'Long Delay'
          |WHEN delay <120 AND delay > 30 THEN 'Short Delay'
          |WHEN delay <30 then 'Tolreable Delay'
          |WHEN delay = 0 THEN 'No delays'
          |ELSE 'Early'
        |END AS Flight_Delays
        | FROM us_delay_flights_tbl
        | ORDER BY origin, delay DESC""".stripMargin)show(10)

    spark.sql("""CREATE DATABASE learn_spark_db""")
    spark.sql("""USE learn_spark_db""")

    //Creación managed table
    spark.sql("""CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)""")


   //Creación unmanaged table
    spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
 distance INT, origin STRING, destination STRING)
 USING csv OPTIONS (PATH
 "C:\\Users\\alejandro.hernaez\\IdeaProjects\\Learning_Spark\\departuredelays.csv")""")

    val flights_df = spark.read.csv("C:\\Users\\alejandro.hernaez\\IdeaProjects\\Learning_Spark\\departuredelays.csv", schema)

    flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

    spark.sql(
      """SELECT * FROM managed_us_delay_flights_tbl""").show(50)


    //creación de vistas
    spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
        SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
        origin = 'SFO'""");
      spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
      SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
        origin = 'JFK'""");

      spark.sql("""SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view""")

      spark.sql("""DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view""");
      spark.sql("""DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view""");

      // In Scala
      //val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
      //val usFlightsDF2 = spark.table("us_delay_flights_tbl")

    //In SQL
    spark.sql("""CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
      SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
      origin = 'SFO'""");
    spark.sql("""CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
    SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
      origin = 'JFK'""");

    spark.sql("""SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view""")

    //In SQL
    spark.sql("""SELECT * FROM us_origin_airport_JFK_tmp_view""")
      // In Scala/Python
      spark.read.table("""us_origin_airport_JFK_tmp_view""")

    spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
    spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

    // In Scala
    val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
    val usFlightsDF2 = spark.table("us_delay_flights_tbl")*/


  }

  def ej2(spark: SparkSession, csvFile: String): Unit = {

    // In Scala
    // Use Parquet
    val file = """/databricks-datasets/learning-spark-v2/flights/summary-
 data/parquet/2010-summary.parquet"""
    val df = spark.read.format("parquet").load(file)
    // Use Parquet; you can omit format("parquet") if you wish as it's the default
    val df2 = spark.read.load(file)
    // Use CSV
    val df3 = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")
    // Use JSON
    val df4 = spark.read.format("json")
      .load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")

  }
}
