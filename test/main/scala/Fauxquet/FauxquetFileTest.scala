package main.scala.Fauxquet

import java.io.{BufferedReader, File, FileReader}

import main.scala.Fauxquet.flare.FauxquetFile
import org.scalatest.FunSuite

/**
  * Created by james on 10/22/16.
  */
class FauxquetFileTest extends FunSuite {
  val CSV_DELIMITER = "|"

  test("timingTest") {
    val numRuns = 1

    var sum = 0L

    for (i <- 0 until numRuns) {
      val t0 = System.nanoTime()

      val fauxquetFile = new FauxquetFile()
      fauxquetFile.read("resources/nation.parquet")
      fauxquetFile.write("resources/nation_out.parquet", fauxquetFile.mtSchema)

      val t1 = System.nanoTime()

      sum += t1 - t0
    }

    println(s"$numRuns took ${sum / 1000000000.0} seconds.")
  }

  test("From CSV to Parquet") {
    val csvFile = new File("resources/customer.csv")
    val parquetFile = new File("resources/customer_out_from_csv.parquet")

    val custSchema = "message m {\n  optional int64 cust_key;\n  optional binary name;\n  optional binary address;\n  optional int32 nation_key;\n  optional binary phone;\n  optional double acctbal;\n  optional binary mktsegment;\n  repeated binary comment_col;\n}"

    val schema = MessageTypeParser.parse(custSchema)

    val br = new BufferedReader(new FileReader(csvFile))

    var lineNumber = 0
    var line = br.readLine()

    val f = new FauxquetFile
    f.mtSchema = schema
    f.write()

    while (line != null) {
      val fields = line.split(CSV_DELIMITER)




      line = br.readLine()
    }
  }
}