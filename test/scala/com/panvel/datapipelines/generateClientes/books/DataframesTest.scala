package com.panvel.datapipelines.generateClientes.books

import com.panvel.datapipelines.generateClientes.commons.session.SparkSessionWrapperTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.{DataFrame, Row}

import java.sql.Date

object DataframesTest extends SparkSessionWrapperTest{

  def FuncaoDataframesTest(): (DataFrame, DataFrame) = {

    val inputSchema: StructType =
      StructType(
        Array(
          StructField("A", StringType, nullable = true),
          StructField("B", LongType, nullable = true),
          StructField("C", DateType, nullable = true),
          StructField("D", IntegerType, nullable = true)
        )
      )

    val outputSchema: StructType =
      StructType(
        Array(
          StructField("A", StringType, nullable = true),
          StructField("B", LongType, nullable = true),
          StructField("C", DateType, nullable = true),
          StructField("D", IntegerType, nullable = true),
          StructField("F", IntegerType, nullable = true)
        )
      )

    val inputValues: Seq[Row] =
      Seq(
        Row("AAA", 111L, Date.valueOf("2024-01-01"), 1),
        Row("AAA", 222L, Date.valueOf("2024-01-01"), 1),
        Row("AAA", 333L, Date.valueOf("2024-01-01"), 1),
        Row("AAA", 444L, Date.valueOf("2024-01-01"), 1),
      )

    val outputValues: Seq[Row] =
      Seq(
        Row("AAA", 111L, Date.valueOf("2024-01-01"), 1, 7),
        Row("AAA", 222L, Date.valueOf("2024-01-01"), 1, 7),
        Row("AAA", 333L, Date.valueOf("2024-01-01"), 1, 7),
        Row("AAA", 444L, Date.valueOf("2024-01-01"), 1, 7),
      )

    val inputDf: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(inputValues), inputSchema)
    val outputDf: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(outputValues), outputSchema)
    val testedDf: DataFrame = inputDf.withColumn("F", lit(7))

    (testedDf, outputDf)
  }

}
