package main.scala.Fauxquet.flare

import java.io.{BufferedOutputStream, Closeable, FileOutputStream, PrintWriter}

import main.scala.Fauxquet.FauxquetObjs._
import main.scala.Fauxquet.FauxquetOutputStream
import main.scala.Fauxquet.flare.api.WriteSupport

/**
  * Created by james on 1/10/17.
  */
class FauxquetWriter(path: String, writeSupport: WriteSupport) extends Closeable {
  val DEFAULT_BLOCK_SIZE: Int = 128 * 1024 *10241
  val DEFAILT_PAGE_SIZE: Int = 1048576 //in a config file somewhere

  val schema = writeSupport.schema

  val fauxquetFileWriter = new FauxquetFileWriter(new FauxquetOutputStream(new BufferedOutputStream(new FileOutputStream(path))), schema)

  fauxquetFileWriter.start()

  val writer = new InternalFauxquetRecordWriter(fauxquetFileWriter, writeSupport, schema, Map[String, String](), DEFAULT_BLOCK_SIZE)

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

  def write(schema: Vector[SchemaElement], data: Map[String, Vector[Any]]) = {
//    val writer = new FauxquetFileWriter(out, schema, NoAlignment)
//
//    for (el <- schema) {
//      val vals = data(el.name)
//      val cwi = new ColumnWriterImpl(new ColumnDescriptor(Array[String](el.name), el.Type, 0, 0, 1), new ColumnChunkPageWriter(new ColumnDescriptor(Array[String](el.name), el.Type, 0, 0, 1)))
//
//      for (v <- vals) {
//        el.Type match {
//          case BOOLEAN => cwi.write(v.asInstanceOf[Boolean], 0, 1)
//          case INT32 => cwi.write(v.asInstanceOf[Int], 0, 1)
//          case INT64 => cwi.write(v.asInstanceOf[Long], 0, 1)
//          case INT96 => throw new Error("Int96 unsupported") /*cwi.write(v.asInstanceOf[INT96], 0, 1)*/
//          case FLOAT => cwi.write(v.asInstanceOf[Float], 0, 1)
//          case DOUBLE => cwi.write(v.asInstanceOf[Double], 0, 1)
//          case BYTE_ARRAY => cwi.write(v.asInstanceOf[Array[Byte]], 0, 1)
//          case FIXED_LEN_BYTE_ARRAY => cwi.write(v.asInstanceOf[Array[Byte]], 0, 1)
//        }
//      }
//    }
  }

  override def close(): Unit = {

  }
}
