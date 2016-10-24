package main.scala.Fauxquet

import org.scalatest.FunSuite

/**
  * Created by james on 10/22/16.
  */
class FauxquetFileTest extends FunSuite {
  test("timingTest") {
    val numRuns = 30

    var sum = 0L

    for (i <- 0 until numRuns) {
      val t0 = System.nanoTime()

      val fauxquetFile = new FauxquetFile("resources/customer.parquet")
      fauxquetFile init()

      val t1 = System.nanoTime()

      sum += t1 - t0
    }

    println(s"$numRuns took ${sum / 1000000000.0} seconds.")
  }
}
