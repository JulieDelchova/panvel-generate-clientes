package com.panvel.datapipelines.generateClientes.jobs

import com.panvel.datapipelines.generateClientes.commons.session.SparkSessionWrapper
import com.panvel.datapipelines.generateClientes.commons.dataframe.RunnableJob
import com.panvel.datapipelines.generateClientes.books.Transformations.generateClientes
import com.panvel.datapipelines.generateClientes.books.Variables._
import com.panvel.datapipelines.generateClientes.commons.dataframe.CommonConstants._
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions._

object ClienteJob extends RunnableJob with SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    runJob()
  }

  override def runJob(): Unit = {
    val conf: Config = ConfigFactory.load

    //carrega tabelas
    val clientesRawDf: DataFrame =
      spark.read.parquet(conf.getString(CLIENTES_RAW_PATH))
        .select(clientesColSeq: _*)

    val clientesOptRawDf: DataFrame =
      spark.read.json(conf.getString(CLIENTES_OPT_RAW_PATH))
        .select(clientesOptColSeq: _*)

    val enderecosClientesRawDf: DataFrame =
      spark.read.parquet(conf.getString(ENDERECOS_CLIENTES_RAW_PATH))
        .select(enderecosClientesColSeq: _*)

    //gera output
    generateClientes(clientesRawDf, clientesOptRawDf, enderecosClientesRawDf)
      .write.mode(SaveMode.Overwrite)
      .parquet(conf.getString(CLIENTES_OUTPUT_PATH))

  }
}
