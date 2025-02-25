package org.example  // Define el paquete donde se encuentra este objeto

import org.apache.spark.sql.functions.{avg, col, concat, expr}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}  // Importa SparkSession, que es necesario para ejecutar Spark

// Definimos el objeto `T_3`, que contiene la función para calcular el promedio de edades
object T_3 {

  /**
   * Función que calcula el promedio de edad por nombre.
   * @param spark Instancia de SparkSession
   */
  def ej1(spark: SparkSession): Unit = {

    // Creación de un DataFrame manual con nombres y edades
    val dataDF = spark.createDataFrame(Seq(
      ("Brooke", 20),  // Persona llamada "Brooke" con edad 20
      ("Brooke", 25),  // Otra persona llamada "Brooke" con edad 25
      ("Denny", 31),   // Persona llamada "Denny" con edad 31
      ("Jules", 30),   // Persona llamada "Jules" con edad 30
      ("TD", 35)       // Persona llamada "TD" con edad 35
    )).toDF("name", "age")  // Definimos los nombres de las columnas como "name" y "age"

    // Agrupar los datos por nombre y calcular el promedio de edad
    val avgDF = dataDF
      .groupBy("name")  // Agrupa los datos por la columna "name"
      .agg(avg("age"))  // Calcula el promedio de la columna "age" para cada grupo

    // Mostrar los resultados en la consola
    avgDF.show()
  }

  //--------------------------------------------------------------------------------------------

  def ej2(spark: SparkSession, jsonFile:String): Unit = {

    // Get the path to the JSON file
    import spark.implicits._
    // Define our schema programmatically
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))
    // Create a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    // Show the DataFrame schema as output
    blogsDF.show(false)
    // Print the schema
    println(blogsDF.printSchema)
    println(blogsDF.schema)

  }

  //--------------------------------------------------------------------------------------------------------------------
  def ej3(spark: SparkSession, jsonFile: String): Unit = {
    // In Scala
    import spark.implicits._
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))
    // Create a DataFrame by reading from the JSON file
    // with a predefined schema
    val blogsDF = spark.read.schema(schema).json(jsonFile)
    blogsDF.columns
    // Access a particular column with col and it returns a Column type
    blogsDF.col("Id")
    // Use an expression to compute a value
    blogsDF.select(expr("Hits * 2")).show(2)
    // or use col to compute value
    blogsDF.select(col("Hits") * 2).show(2)
    // Use an expression to compute big hitters for blogs
    // This adds a new column, Big Hitters, based on the conditional expression
    blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
    // Concatenate three columns, create a new column, and show the
    // newly created concatenated column
    blogsDF
      .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
      .select(col("AuthorsId"))
      .show(4)
    // These statements return the same value, showing that
    // expr is the same as a col method call
    blogsDF.select(expr("Hits")).show(2)
    blogsDF.select(col("Hits")).show(2)
    blogsDF.select("Hits").show(2)
    // Sort by column "Id" in descending order
    blogsDF.sort(col("Id").desc).show()
    blogsDF.sort($"Id".desc).show()

  }

  //---------------------------------------------------------------------------------------------------------------
  def ej4(spark: SparkSession): Unit = {


    import org.apache.spark.sql.Row
    // Create a Row
    val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
      Array("twitter", "LinkedIn"))
    // Access using index for individual items
    println(blogRow)
  }



}