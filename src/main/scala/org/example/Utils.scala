package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {

  def csvAdf(spark: SparkSession, csvFile: String): DataFrame = {
    // Lee el archivo CSV y lo carga en un DataFrame
    val df: DataFrame= spark.read.format("csv") // Archivo CSV
      .option("header", "true") // Nombres de las columnas
      .option("inferSchema", "true") // Spark gestiona los tipos de datos
      .load(csvFile) // Carga el archivo en un DataFrame


    df // Devuelve DataFrame
  }

  def jsonAdf(spark: SparkSession, jsonFile: String): DataFrame = {
    // Lee el archivo JSON y lo carga en un DataFrame
    val df: DataFrame = spark.read.format("json") // Archivo JSON
      .option("inferSchema", "true") // Spark gestiona los tipos de datos
      .load(jsonFile) // Carga el archivo en un DataFrame

    //LA LINEA DE HEADER NO ES NECESARIA PARA JSON

    df // Create new scratch file from selection el DataFrame
  }

}
