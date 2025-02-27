package org.example

import org.apache.spark.sql.SparkSession
import java.util.Properties


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



    //--------------------------------------------------------------------------------------


    // Ejecutar funciones
    spark.sql("""SELECT id, cubed(id) AS id_cubed FROM udf_test""").show()

  }
  //--------------------------------------------------------------------------------------

  //Ejercicios windowing

  //--------------------------------------------------------------------------------------
  /*
  //Os pediria como ejercicio que os abrierais una spark shell en local, os crearais 2 dataframes simples
    // e hicierais por ejemplo un join entre los 2 dataframes y luego hicierais ese mismo
    // join pero con spark.sql y a traves de vistas temporales. luego aplicando el .explain comparar lo que spark hace por debajo

  //Esto va a terminal
  // Crear el primer DataFrame
    val data1 = Seq((1, "Mario"), (2, "Luigi"), (3, "Peach"))
    val df1 = data1.toDF("id", "name")
    // Crear el segundo DataFrame (departamentos)
    val data2 = Seq((1, "A"), (2, "B"), (4, "C"))
    val df2 = data2.toDF("id", "department")
    // Realizar el JOIN usando la API de DataFrame
    val df_join = df1.join(df2, Seq("id"), "inner")
    // Mostrar resultados
    df_join.show()


 */
  //-------------------------------------------------------------------------


  def ej2(spark: SparkSession): Unit = {
    import spark.implicits._

    // Crear DataFrames dentro de ej2
    val data1 = Seq((1, "Mario"), (2, "Luigi"), (3, "Peach"))
    val df1 = data1.toDF("id", "name")

    val data2 = Seq((1, "A"), (2, "B"), (4, "C"))
    val df2 = data2.toDF("id", "department")

    // Registrar DataFrames como vistas temporales
    df1.createOrReplaceTempView("employees")
    df2.createOrReplaceTempView("departments")

    // Ejecutar el JOIN en Spark SQL
    val sql_query = spark.sql(
      """SELECT e.id, e.name, d.department
      FROM employees e
      INNER JOIN departments d ON e.id = d.id
      """
    )

    // Mostrar resultados
    sql_query.show()
  }
  /*def ej3(spark: SparkSession): Unit = {
    // Cargar los datos en un DataFrame (esto es solo un ejemplo)
    val departureDelays = spark.read.option("header", "true")
      .csv("") //

    // Crear vista temporal de departureDelays
    departureDelays.createOrReplaceTempView("departureDelays")

    // Ejecutar la consulta en SQL sobre la vista temporal
    val result = spark.sql("""
    SELECT origin, destination, SUM(delay) AS TotalDelays
    FROM departureDelays
    WHERE origin IN ('SEA', 'SFO', 'JFK')
    AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
    GROUP BY origin, destination
  """)

    // Mostrar los resultados
    result.show()
  }*/

  def ej_windowing(spark: SparkSession): Unit = {

    val csvCoches = "C:\\Users\\alejandro.hernaez\\IdeaProjects\\Learning_Spark\\COCHES.csv"
    val df = Utils.csvAdf(spark,csvCoches)
    df.createOrReplaceTempView("coches")
    // leer csv , crear df con la funcion de Utils y crear vista temporal

    spark.sql(
      """
        |SELECT name,
        |       company,
        |       power,
        |       row_number() OVER (ORDER BY power DESC) AS row_number,
        |       rank() OVER (ORDER BY power DESC) AS rank,
        |       dense_rank() OVER (ORDER BY power DESC) AS dense_rank
        |FROM coches
        |""".stripMargin).show()




  }

}
