# Databricks notebook source
# MAGIC %md
# MAGIC ##Montaje de contenedores utilizando dbutils
# MAGIC

# COMMAND ----------

dbutils.fs.mount(
  source="wasbs://ctd1@jcartesstore.blob.core.windows.net",
  mount_point="/mnt/conte1",
  extra_configs={
    "fs.azure.account.key.jcartesstore.blob.core.windows.net":"jQLyd7zfrefKGSQY1N+GynFu9es+bmQa3uKO/LatclpprkFU+M12T4edEuuvjps4YCMYSuN0TTWP+ASt3mT05A=="
  }
)




# COMMAND ----------

dbutils.fs.mount(
  source="wasbs://ctd2@jcartesstore.blob.core.windows.net",
  mount_point="/mnt/conte2",
  extra_configs={
    "fs.azure.account.key.jcartesstore.blob.core.windows.net":"jQLyd7zfrefKGSQY1N+GynFu9es+bmQa3uKO/LatclpprkFU+M12T4edEuuvjps4YCMYSuN0TTWP+ASt3mT05A=="
  }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Revision de Puntos de montaje

# COMMAND ----------

dbutils.fs.ls("/mnt/conte1")


# COMMAND ----------

dbutils.fs.ls("/mnt/conte2")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Revision de todos los puntos de montaje

# COMMAND ----------

ruta_montaje = dbutils.fs.mounts()
print(ruta_montaje)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Probando cargando archivo como dataframe utilizando spark

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("/mnt/conte1/ds_salaries.csv")

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ##Creacion de Base de datos en el segundo contenedor utilizando pyspark

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.getOrCreate()

# Definir el nombre y la ubicación de la base de datos
database_name = "BD2"
database_location = "/mnt/conte2/BD2"

# Crear la base de datos
spark.sql(f"CREATE DATABASE {database_name} LOCATION '{database_location}'")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Creacion de Tabla en la base de datos y en el contenedor 2 utilizando pyspark

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.getOrCreate()

# Definir el nombre de la base de datos y la tabla
database_name = "BD2"
table_name = "Tabla1"

# Especificar la ruta del archivo en ctr1
file_path = "/mnt/conte1/ds_salaries.csv"

# Leer el archivo CSV y crear un DataFrame
df = spark.read.format("csv").option("header", "true").load(file_path)

# Especificar la ruta de almacenamiento de la tabla
table_location = f"/mnt/conte2/{database_name}/{table_name}"

# Guardar el DataFrame como tabla
df.write.mode("overwrite").format("csv").save(table_location)

# Registrar la tabla en la base de datos
spark.sql(f"USE {database_name}")
spark.sql(f"CREATE TABLE {table_name} USING CSV LOCATION '{table_location}'")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Consultas utilizando pyspark y visualizacion de informacion respectiva

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.getOrCreate()

# Definir el nombre de la base de datos y la tabla
database_name = "BD2"
table_name = "Tabla1"

# Seleccionar la base de datos
spark.sql(f"USE {database_name}")

# Ejecutar una consulta SQL para ver los datos de la tabla
df = spark.sql(f"SELECT * FROM {table_name}")

# Mostrar los datos del DataFrame
df.show()

