package com.panvel.datapipelines.generateClientes.books

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import com.panvel.datapipelines.generateClientes.commons.session.SparkSessionWrapper
import com.panvel.datapipelines.generateClientes.books.Functions
import com.panvel.datapipelines.generateClientes.books.Constants._
import com.panvel.datapipelines.generateClientes.books.Variables._

object Transformations extends SparkSessionWrapper {

  def generateClientes(clientesRawDf: DataFrame,
                       clientesOptRawDf: DataFrame,
                       enderecosClientesRawDf: DataFrame): DataFrame = {

    val functions: Functions = new Functions

    val clientesTransformedDf: DataFrame =
      clientesRawDf
        .dropDuplicates()
        .filter(col(CODIGO_CLIENTE).isNotNull)
        .transform(functions.minus2StringTreatment(minus2ClienteSeq))

    val clientesOptTransformedDf: DataFrame =
      clientesOptRawDf
        .dropDuplicates()
        .filter(col(CODIGO_CLIENTE).isNotNull)
        .transform(functions.columnCreationEngine(flagVarsRules))
        .transform(functions.minus2FlagVarsTreatment)

    val enderecosClientesTransformedDf: DataFrame =
      enderecosClientesRawDf
        .dropDuplicates()
        .filter(col(CODIGO_CLIENTE).isNotNull)
        .transform(functions.columnCreationEngine(endVarRules))
        .transform(functions.minus2StringTreatment(minus2EnderecoSeq))

    val finalClientesDf: DataFrame =
      clientesTransformedDf
        .join(clientesOptTransformedDf, Seq(CODIGO_CLIENTE), "left")
        .join(enderecosClientesTransformedDf, Seq(CODIGO_CLIENTE), "left")
        .transform(functions.createIdadeCol)
        .transform(functions.minus1TreatNull)
        .transform(functions.filterNullRows)

    finalClientesDf
  }
}