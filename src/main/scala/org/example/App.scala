package org.example  // Define el paquete del programa para organizar el código

import org.apache.spark.sql.SparkSession
// Importa SparkSession, la entrada principal para Spark

// Definimos el objeto `App`, que contiene la función principal del programa
object App {


  def main(args: Array[String]): Unit = {

    // Creamos una instancia de SparkSession, necesaria para ejecutar operaciones en Spark
    val spark = SparkSession
      .builder()  // Inicia el constructor de SparkSession
      .appName("App")  // Asigna el nombre "App" a la sesión de Spark (aparece en la interfaz de Spark UI)
      .master("local[*]")  // Ejecuta Spark en modo local usando todos los núcleos disponibles del procesador
      .getOrCreate()  // Si ya hay una sesión de Spark en ejecución, la reutiliza; si no, la crea

    /*val spark = SparkSession.builder()
      .appName("SparkWithoutHadoop")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Use local storage
      .getOrCreate()*/

    // Ruta del archivo CSV que contiene los datos de los M&M
   // val mnmFile = "C:\\Users\\alejandro.hernaez\\IdeaProjects\\Learning_Spark\\mnm_dataset.csv"
    // NOTA: Se usa `\\` en Windows porque `\` es un carácter de escape en cadenas de texto en Scala

    // Llamamos a la función `ej_1` del objeto `MnMcount` para procesar los datos
    //MnMcount.ej_1(spark, mnmFile)  // Ejecuta el análisis de conteo de M&M por color y estado

    //-------------------------------------------------------------------------------------------

    //Llamada a función ej_1 del objeto T_3
    //T_3.ej1(spark)

    //-------------------------------------------------------------------------------------------

    //Llamada a función ej_2 T_3
    //val jsonFile = args(0) // Ahora jsonFile está disponible fuera del if

    //T_3.ej2(spark, jsonFile)

    //--------------------------------------------------------------------------------------------
    //val jsonFile = args(0)

    //T_3.ej3(spark, jsonFile)

    //--------------------------------------------------------------------------------------------

    //T_3.ej4(spark)
    // Una vez finalizado el procesamiento, detenemos la sesión de Spark para liberar recursos

    //--------------------------------------------------------------------------------------------
    //val csvFile="""C:\Users\alejandro.hernaez\IdeaProjects\Learning_Spark\departuredelays.csv"""

    //T_4.ej1(spark, csvFile )  //VUELOS Y RETRASOS

    //---------------------------------------------------------------------------------------------

   //val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"""

    //T_4.ej2(spark, file )

    //--------------------------------------------------------------------------------------------

   // T_5.ej1(spark)

    //T_5.ej2(spark)

    T_5.ej3(spark)



    spark.stop()
  }
}

