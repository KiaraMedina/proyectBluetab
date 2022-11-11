// Databricks notebook source
// dbutils.fs.mount(
//   source = "wasbs://bronze@datalakebluetab.blob.core.windows.net/a3825ce2-adf2-47a8-bfd8-f065e100275b",
//   mountPoint = "/mnt/bronze",
//   extraConfigs = Map("fs.azure.sas.bronze.datalakebluetab.blob.core.windows.net" -> dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")))


// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType,DoubleType, DateType};
import org.apache.spark.sql.functions.{to_date,lit}

// COMMAND ----------

// MAGIC %python
// MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
// MAGIC        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC        "fs.azure.account.oauth2.client.id": "67ec92af-e910-43ca-910f-925a35d4a7e6",
// MAGIC        "fs.azure.account.oauth2.client.secret": "1AU8Q~ui4NK8XGGCL4HUUOfKqKWeA8Bek9a7Ybwy",
// MAGIC        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/a3825ce2-adf2-47a8-bfd8-f065e100275b/oauth2/token",
// MAGIC        "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC dbutils.fs.mount(
// MAGIC   source = "abfss://bronze@datalakebluetab.dfs.core.windows.net/",
// MAGIC   mount_point = "/mnt/datalakebluetab/bronze",
// MAGIC   extra_configs = configs)
// MAGIC 
// MAGIC dbutils.fs.mount(
// MAGIC   source = "abfss://silver@datalakebluetab.dfs.core.windows.net/",
// MAGIC   mount_point = "/mnt/datalakebluetab/silver",
// MAGIC   extra_configs = configs)

// COMMAND ----------

val df_schema_pago_cabecera = 
  StructType(
    Array(
      StructField("NUMERO_MOVIMIENTO", IntegerType, true ),
      StructField("TIPO_DOCUMENTO", StringType, true ),
      StructField("NUMERO_DOCUMENTO", IntegerType, true ),
      StructField("CODIGO_PERSONA", IntegerType, true ),
      StructField("FECHA_EMISION", StringType, true ),
      StructField("FECHA_DOCUMENTO", StringType, true ),
      StructField("IMPORTE_DOCUMENTO", DoubleType, true ),
      StructField("ID_TIPO_MODENA_DOCUMENTO", IntegerType, true ),
      StructField("TIPO_MODENA_DOCUMENTO", StringType, true ),
      StructField("FECHA_PAGO", StringType, true ),
      StructField("CODIGO_TIPO_PAGO", StringType, true ),
      StructField("IMPORTE_PAGO", DoubleType, true ),
      StructField("ID_MODENA_DOCUMENTO", IntegerType, true ),
      StructField("TIPO_MODENA_PAGO", StringType, true ),
      StructField("TIPO_CAMBIO", DoubleType, true )
))

// COMMAND ----------

// val iter = dbutils.fs.ls("/mnt/datalakebluetab/bronze/")
// var contador = 1
// for(file <- 0 to iter.length)
// {
//   var ruta_origen_bronz = "/mnt/datalakebluetab/bronze/PagoCabecera_G"+contador+".csv"
//   var df = spark.read
//       .format("csv")
//       .schema(df_schema_pago_cabecera)
//       .options( Map(
//       "header" -> "true",
//       "sep" -> ","
//       ))
//       .load(ruta_origen_bronz)
//   contador == contador+1
// }
// // list.length
// display(df)
//nuevo comentario


// COMMAND ----------

  val ruta_origen_bronz = "/mnt/datalakebluetab/bronze/PagoCabecera_*.csv"
  val df_2 = spark.read
      .format("csv")
      .schema(df_schema_pago_cabecera)
      .options( Map(
      "header" -> "true",
      "sep" -> ","
      ))
      .load(ruta_origen_bronz)
display(df2)

// COMMAND ----------

df2.printSchema

// COMMAND ----------

var df_silver = df2
.select($"NUMERO_MOVIMIENTO",$"TIPO_DOCUMENTO", $"TIPO_MODENA_DOCUMENTO",$"IMPORTE_PAGO",$"FECHA_PAGO")
// .groupBy($"TIPO_DOCUMENTO")

// COMMAND ----------

display(df_silver.sum("IMPORTE_PAGO"))

// COMMAND ----------

val df_silver = df.repartition($"TIPO_DOCUMENTO")
val ruta_destino_silver = "/mnt/datalakebluetab/silver"
df_silver.write.mode("overwrite").format("parquet").save(ruta_destino_silver)

// COMMAND ----------

val cosmosCfg = Map("spark.cosmos.accountEndpoint" -> "https://cosmosdbbluetab.documents.azure.com:443/",
       "spark.cosmos.accountKey" -> "FoK80u24JWq62PZsUJW72eXMOHDWGcRAhahf6Wyc82RCY7itCMNlQUMg9TAQf2Rrw67dSCVNKOyHACDbvj87hg==",
        "spark.cosmos.database" -> "BLUETAB",
        "spark.cosmos.container" -> "Personal"
   ) 

// COMMAND ----------

df_silver
.write
.format("cosmos.oltp")//indica el formato tipo de libreria tipo cosmos 
.options(cosmosCfg)
.mode("APPEND")
.save()

// COMMAND ----------


