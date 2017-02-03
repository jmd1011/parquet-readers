package main.scala.Fauxquet.flare

import main.scala.Fauxquet.FauxquetObjs.{BIT_PACKED, PLAIN, RLE}
import main.scala.Fauxquet.FauxquetObjs.statistics.BinaryStatistics
import main.scala.Fauxquet.bytes.BytesInput.BytesInput
import main.scala.Fauxquet.schema._
import org.scalatest.FunSuite

/**
  * Created by james on 2/1/17.
  */
class TestFauxquetFileWriter extends FunSuite {
  test("testWriteRead") {
    val BYTES1: Array[Byte] = Array[Byte](0, 1, 2, 3)
    val BYTES2: Array[Byte] = Array[Byte](1, 2, 3, 4)
    val BYTES3: Array[Byte] = Array[Byte](2, 3, 4, 5)
    val BYTES4: Array[Byte] = Array[Byte](3, 4, 5, 6)

    val mt = new MessageType("m", {
      var l = List[PrimitiveType]()

      l ::= new PrimitiveType(REQUIRED, BINARY, 0, "name", null, null, null)
      l ::= new PrimitiveType(REQUIRED, INT32, 0, "id", null, null, null)
      l ::= new PrimitiveType(REQUIRED, DOUBLE, 0, "cash_money", null, null, null)

      l
    })

    val filePath = "resources/test.parquet"
    val w = new FauxquetFileWriter(filePath, mt)
    w.start()
    w.startBlock(3)
    w.startColumn(mt.getColumnDescription(Array[String]("name")), 2)

    val t1 = w.pos

    w.writeDataPage(2, 4, BytesInput.from(BYTES1), new BinaryStatistics, BIT_PACKED, RLE, PLAIN)
    w.writeDataPage(3, 4, BytesInput.from(BYTES1), new BinaryStatistics, BIT_PACKED, RLE, PLAIN)
    w.endColumn()

    val t2 = w.pos


  }
}
