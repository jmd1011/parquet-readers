package main.scala.Fauxquet

import java.io.{BufferedOutputStream, FileOutputStream, PrintWriter}
import java.nio.charset.Charset

import main.scala.Fauxquet.FauxquetObjs.ColumnDescriptor

/**
  * Created by james on 1/10/17.
  */
class FauxquetWriter(path: String) {


  def writeToCSV(data: Map[String, Vector[Any]]) = {
    val out = new PrintWriter("./resources/customer_out.csv")
    var i = 0
    var keepGoing = true

    val order = Vector[String]("cust_key", "name", "address", "nation_key", "phone", "acctbal", "mktsegment", "comment_col") //should be able to get this from Parquet file somehow

    while (keepGoing) {
      if (i != 0) out.write("\n")

      for (col <- order) {
        if (data(col).length <= i) {
          keepGoing = false
        }
        else {
          val d = data(col)(i)
          out.write(d.toString)

          if (col != order.last) {
            out.write("|")
          }
        }
      }

      i += 1
    }

    out.close()
  }

  def write(data: Map[String, Vector[Any]]) = {

  }


}
