package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {

  def csvAdf(spark: SparkSession, csvFile: String): DataFrame = {
    // Lee el archivo CSV y lo carga en un DataFrame
    val df: DataFrame= spark.read.format("csv") // Especificamos que el archivo es CSV
      .option("header", "true") // Indica que la primera fila del CSV contiene los nombres de las columnas
      .option("inferSchema", "true") // Permite que Spark infiera automáticamente los tipos de datos
      .load(csvFile) // Carga el archivo en un DataFrame


    df // Retornamos el DataFrame
  }

  def jsonAdf(spark: SparkSession, jsonFile: String): DataFrame = {
    // Lee el archivo JSON y lo carga en un DataFrame
    val df: DataFrame = spark.read.format("json") // Especificamos que el archivo es JSON
      .option("inferSchema", "true") // Permite que Spark infiera automáticamente los tipos de datos
      .load(jsonFile) // Carga el archivo en un DataFrame

    df // Retornamos el DataFrame
  }

}
