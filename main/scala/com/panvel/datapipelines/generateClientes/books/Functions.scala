package com.panvel.datapipelines.generateClientes.books

import com.panvel.datapipelines.generateClientes.books.Constants._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import com.panvel.datapipelines.generateClientes.books.Variables._

class Functions {

  def trimString(columnName: String): Column = {
    trim(col(columnName))
  }

  def createIdadeCol(df: DataFrame): DataFrame = {
    df
      .withColumn(IDADE,
        floor(datediff(current_date(), col(DATA_NASCIMENTO)) / 365).cast("int"))
  }

  /** treatFlagVars function converts false/true to 0/1 respectively in Flag columns
   *
   * @param columnName
   * @return
   */
  def treatFlagVars(columnName: String): Column = {
    when(col(columnName) === "false", "0")
      .otherwise(
        when(col(columnName) === "true", "1")
          .otherwise(null))
  }

  /** minus2FlagVarsTreatment function receves a Sequence of column names and fill null values with -2.
   * It works only to Integer columns
   *
   * @param df
   * @return DataFrame treated
   */
  def minus2FlagVarsTreatment(df: DataFrame): DataFrame = {
    df.na.fill(-2, minus2FlagVarsSeq)
  }

  /** minus2StringTreatment function receves a Sequence of column names and fill null values with '-2'.
   * It works only to String columns
   *
   * @param minus2Seq
   * @param df
   * @return DataFrame treated
   */
  def minus2StringTreatment(minus2Seq: Seq[String])(df: DataFrame): DataFrame = {
    df.na.fill("-2", minus2Seq)
  }

  /** minus1TreatNull function fill null values with -1 or '-1'
   *
   * @param df
   * @return A DataFrame treated
   */
  def minus1TreatNull(df: DataFrame): DataFrame = {
    df.na.fill("-1", stringColSeq)
      .na.fill(-1, integerColSeq)
  }

  def filterNullRows(df: DataFrame): DataFrame = {
    df.filter(
      !(col(DATA_NASCIMENTO).isNull &&
        col(SEXO).isin("-1", "-2", null) &&
        col(ESTADO_CIVIL).isin("-1", "-2", null) &&
        col(FLAG_LGPD_CALL).isin(-1, -2, null) &&
        col(FLAG_LGPD_SMS).isin(-1, -2, null) &&
        col(FLAG_LGPD_EMAIL).isin(-1, -2, null) &&
        col(FLAG_LGPD_PUSH).isin(-1, -2, null) &&
        col(CIDADE).isin("-1", "-2", null) &&
        col(UF).isin("-1", "-2", null) &&
        col(IDADE).isin(-1, -2, null))
    )
  }

  /** columnCreationEngine function is a engine to create multiples columns based on a Seq with
   * the column name and column rule.
   *
   * @param columnsAndRules
   * @param df
   * @return a Dataframe with new columns created
   */
  def columnCreationEngine(columnsAndRules: Seq[(String, Column)])(df: DataFrame): DataFrame = {
    val columnsToDrop =
      columnsAndRules.map(
        x => x._1
      )

    df.select(
      df.columns.diff(columnsToDrop).map(col) ++
        columnsAndRules.map(
            x => x._2.as(x._1)
        ): _*
    )
  }

}
