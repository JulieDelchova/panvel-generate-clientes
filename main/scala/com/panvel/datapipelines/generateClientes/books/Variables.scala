package com.panvel.datapipelines.generateClientes.books

import com.panvel.datapipelines.generateClientes.books.Constants._
import com.panvel.datapipelines.generateClientes.books.Functions
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object Variables {

  val functions: Functions = new Functions

  /*
    clientesColSeq is the Seq of columns to select columns from clientes raw table
  */
  val clientesColSeq = Seq(
    col(V_ID_CLI).cast(StringType).as(CODIGO_CLIENTE),
    col(D_DT_NASC).cast(DateType).as(DATA_NASCIMENTO),
    col(V_SX_CLI).cast(StringType).as(SEXO),
    col(N_EST_CVL).cast(StringType).as(ESTADO_CIVIL)
  )

  /*
    clientesOptColSeq is the Seq of columns to select columns from clientes_opt raw table
  */
  val clientesOptColSeq = Seq(
    col(V_ID_CLI).cast(StringType).as(CODIGO_CLIENTE),
    col(B_PUSH).cast(StringType).as(FLAG_LGPD_CALL),
    col(B_SMS).cast(StringType).as(FLAG_LGPD_SMS),
    col(B_EMAIL).cast(StringType).as(FLAG_LGPD_EMAIL),
    col(B_CALL).cast(StringType).as(FLAG_LGPD_PUSH)
  )

  /*
    enderecosClientesColSeq is the Seq of columns to select columns from enderecos_clientes raw table
  */
  val enderecosClientesColSeq = Seq(
    col(V_ID_CLI).cast(StringType).as(CODIGO_CLIENTE),
    col(V_LCL).cast(StringType).as(CIDADE),
    col(V_UF).cast(StringType).as(UF)
  )

  /*
   flagVarsRules is the Seq with the creation rules of FLAG_LGPD_CALL, FLAG_LGPD_SMS,
   FLAG_LGPD_EMAIL, FLAG_LGPD_PUSH variables
  */
  val flagVarsRules: Seq[(String, Column)] =
    Seq(
      (FLAG_LGPD_CALL, functions.treatFlagVars(FLAG_LGPD_CALL).cast(IntegerType)),
      (FLAG_LGPD_SMS, functions.treatFlagVars(FLAG_LGPD_SMS).cast(IntegerType)),
      (FLAG_LGPD_EMAIL, functions.treatFlagVars(FLAG_LGPD_EMAIL).cast(IntegerType)),
      (FLAG_LGPD_PUSH, functions.treatFlagVars(FLAG_LGPD_PUSH).cast(IntegerType))
    )

  val endVarRules: Seq[(String, Column)] =
    Seq(
      (CIDADE, functions.trimString(CIDADE)),
      (UF, functions.trimString(UF))
    )

  def minus2FlagVarsSeq: Seq[String] =
    Seq(
      FLAG_LGPD_CALL,
      FLAG_LGPD_SMS,
      FLAG_LGPD_EMAIL,
      FLAG_LGPD_PUSH
    )

  def minus2ClienteSeq: Seq[String] =
    Seq(
      SEXO,
      ESTADO_CIVIL
    )

  def minus2EnderecoSeq: Seq[String] =
    Seq(
      CIDADE,
      UF
    )

  def stringColSeq: Seq[String] =
    Seq(
      SEXO,
      UF,
      CIDADE,
      ESTADO_CIVIL
    )

  def integerColSeq: Seq[String] =
    Seq(
      IDADE,
      FLAG_LGPD_CALL,
      FLAG_LGPD_SMS,
      FLAG_LGPD_EMAIL,
      FLAG_LGPD_PUSH
    )
}