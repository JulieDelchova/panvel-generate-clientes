package com.panvel.datapipelines.generateClientes.commons.session

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val spark: SparkSession = {
    SparkSession.builder()
      .appName("GenerateClienteApp")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
      .config("spark.hadoop.io.nativeio", "false")
      .master("local[*]")
      .getOrCreate()
  }
}
