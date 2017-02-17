package main.scala.Fauxquet.flare

import java.io.{BufferedOutputStream, Closeable, FileOutputStream, PrintWriter}

import main.scala.Fauxquet.FauxquetObjs._
import main.scala.Fauxquet.FauxquetOutputStream
import main.scala.Fauxquet.flare.api.WriteSupport

/**
  * Created by james on 1/10/17.
  */
class FauxquetWriter(path: String, writeSupport: WriteSupport) extends Closeable {
  val DEFAULT_BLOCK_SIZE: Int = 128 * 1024 * 1024
  val DEFAULT_PAGE_SIZE: Int = 1024 * 1024 //in a config file somewhere

  val schema = writeSupport.schema

  val fauxquetFileWriter = new FauxquetFileWriter(path, schema)

  fauxquetFileWriter.start()

  val writer = new InternalFauxquetRecordWriter(fauxquetFileWriter, writeSupport, schema, Map[String, String](), DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE)

  //val out = new FauxquetOutputStream(new BufferedOutputStream(new FileOutputStream(path)))

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

  def write(values: List[String]): Unit = {
    writer.write(values)
  }

  def write(values: Map[Long, Map[String, String]]): Unit = {
    writer.write(values)
  }

  override def close(): Unit = {
    this.writer.close()
  }
}
