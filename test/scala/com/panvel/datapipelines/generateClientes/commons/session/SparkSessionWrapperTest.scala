package com.panvel.datapipelines.generateClientes.commons.session

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapperTest {
  val spark: SparkSession = {
    SparkSession.builder()
      .appName("Clientes Test")
      .master("local[1]")
      .getOrCreate()
  }
}
