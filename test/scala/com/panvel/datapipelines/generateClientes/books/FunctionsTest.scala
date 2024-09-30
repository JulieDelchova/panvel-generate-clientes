package com.panvel.datapipelines.generateClientes.books

import com.panvel.datapipelines.generateClientes.commons.session.SparkSessionWrapperTest
import com.panvel.datapipelines.generateClientes.books.DataframesTest._
import org.scalatest.flatspec.AnyFlatSpec

class FunctionsTest extends AnyFlatSpec with SparkSessionWrapperTest {

  "FuncaoDataframes" should "to return a new 'F' column with 7" in {
    val (testedDf, outputDf) = FuncaoDataframesTest()
    assert(testedDf.collect.sameElements(outputDf.collect))
  }

}
