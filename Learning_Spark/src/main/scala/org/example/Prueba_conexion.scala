package org.example
import org.apache.spark.sql.{SparkSession, DataFrame}
import java.util.Properties


  object Prueba_conexion {

    def main(args: Array[String]): Unit = {
      // Crear una sesión de Spark
      val spark = SparkSession.builder()
        .appName("Postgres Connection Example")
        .master("local[*]") // Esto es solo para una ejecución local
        .getOrCreate()

      // Parámetros de conexión a PostgreSQL
      val jdbcUrl = "jdbc:postgresql://localhost:5432/Console_Games" // Cambia por tu URL de conexión
      val dbProperties = new Properties()
      dbProperties.setProperty("user", "postgres") // Cambia por tu usuario
      dbProperties.setProperty("password", "Contra.1") // Cambia por tu contraseña
      dbProperties.setProperty("driver", "org.postgresql.Driver")

      // Leer datos desde una tabla en PostgreSQL
      val df: DataFrame = spark.read
        .jdbc(jdbcUrl, "console_games", dbProperties) // Cambia el nombre de la tabla

      // Mostrar los primeros registros
      df.show()

      // Detener la sesión de Spark
      spark.stop()
    }
  }

