package org.example  // Define el paquete donde se encuentra este objeto

import org.apache.spark.sql.{SparkSession, DataFrame}  // Importa SparkSession y DataFrame
import org.apache.spark.sql.functions._  // Importa funciones de Spark como sum, count, col, desc

// Definimos el objeto MnMcount, que contiene la función para analizar los datos
object MnMcount {


  def ej_1(spark: SparkSession, mnmFile: String): Unit = {

    // Lee el archivo CSV y lo carga en un DataFrame
    val mnmDF: DataFrame = spark.read.format("csv")  // Especificamos que el archivo es CSV
      .option("header", "true")  // Indica que la primera fila del CSV contiene los nombres de las columnas
      .option("inferSchema", "true")  // Permite que Spark infiera automáticamente los tipos de datos
      .load(mnmFile)  // Carga el archivo en un DataFrame

    // Agregaciones de colores por estado
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")  // Seleccionamos las columnas necesarias
      .groupBy("State", "Color")  // Agrupamos los datos por estado y color
      .agg(sum("Count").alias("Total"))  // Sumamos los valores de "Count" y los renombramos como "Total"
      .orderBy(desc("Total"))  // Ordenamos los resultados en orden descendente según "Total"

    // Mostrar los resultados completos
    countMnMDF.show(60)  // Muestra los primeros 60 registros en la consola
    println(s"Total Rows = ${countMnMDF.count()}")  // Imprime la cantidad total de filas obtenidas
    println()  // Línea en blanco para mejorar la legibilidad

    // Filtrar solo datos de California
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")  // Seleccionamos nuevamente las columnas necesarias
      .where(col("State") === "CA")  // Filtramos solo las filas donde el estado es "CA" (California)
      .groupBy("State", "Color")  // Agrupamos por estado y color (en este caso, solo "CA")
      .agg(sum("Count").alias("Total"))  // Sumamos la cantidad de M&M de cada color
      .orderBy(desc("Total"))  // Ordenamos los resultados en orden descendente según "Total"

    // Mostrar los resultados filtrados de California
    caCountMnMDF.show(10)  // Muestra los primeros 10 registros en la consola
  }
}
