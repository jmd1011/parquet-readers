package main.scala.Fauxquet.flare

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import main.scala.Fauxquet.FauxquetObjs._
import main.scala.Fauxquet.{LittleEndianDecoder, SeekableArray}
import main.scala.Fauxquet.ValuesReaders.bitpacking.ByteBitPackingValuesReader
import main.scala.Fauxquet.ValuesReaders.rle.RunLengthBitPackingValuesReader

import scala.collection.mutable

/**
  * Created by james on 1/10/17.
  */
class FauxquetReader(val path: String) {
  val fileMetaData = new FileMetadata()
  lazy val array = new SeekableArray[Byte](Files.readAllBytes(Paths.get(path)))
  val MAGIC = "PAR1".getBytes(Charset.forName("ASCII"))

  def init() = {
    if (!isParquetFile) throw new Error(s"$path is not a valid Parquet file.")

    fileMetaData read array

    array pos = 4
  }

  def read(): Map[String, Vector[Any]] = { //will change to be Record type
    var data: mutable.Map[String, Vector[Any]] = new mutable.HashMap[String, Vector[Any]]()

    //    val data: Array[Array[Any]] = new Array[Array[Any]](fileMetaData.schema.length)
    //var numRead = 0
    //val inds: Array[Int] = new Array[Int](fileMetaData.schema.length)

    //    for (i <- 1 until fileMetaData.schema.length) {
    //      data(i) = new Array[Any](fileMetaData.numRows.toInt)
    //    }

    var test = 0
    var maxSkip = 0

    for (rg <- fileMetaData.rowGroups) {
      //var i = 1
      for (ci <- rg.columns.indices) {
        val cc = rg.columns(ci)
        var valuesRead = 0L
        val col = fileMetaData.schema(ci + 1).name

        if (!data.contains(col)) {
          data += (col -> Vector[Any]())
        }

        while (valuesRead < rg.numRows) {
          val pageHeader = new PageHeader
          pageHeader read array

          valuesRead += pageHeader.dataPageHeader.numValues

          val repetitionReader = new ByteBitPackingValuesReader(0)
          repetitionReader.initFromPage(rg.numRows.asInstanceOf[Int], array.array, array.pos)

          val definitionReader = new RunLengthBitPackingValuesReader(1)
          definitionReader.initFromPage(rg.numRows.asInstanceOf[Int], array.array, array.pos)

          //for alignment
          val numToSkip = LittleEndianDecoder readInt array
          array.pos = array.pos + numToSkip
          maxSkip = math.max(maxSkip, numToSkip)

          var j = 0
          while (j < pageHeader.dataPageHeader.numValues) {
            test += 1

            val r = repetitionReader.readInt()
            val d = definitionReader.readInt()

            if (d > fileMetaData.schema(ci + 1).definition)
              println("<NULL>")
            else
              cc.metadata.Type match {
                case BOOLEAN => data(col) :+= LittleEndianDecoder readBool array
                case INT32 => data(col) :+= LittleEndianDecoder readInt array
                case INT64 => data(col) :+= LittleEndianDecoder readLong array
                case INT96 => data(col) :+= /*fileMetaData.schema(i).Type.value +*/ " should be int96"
                case FLOAT => data(col) :+= LittleEndianDecoder readFloat array
                case DOUBLE => data(col) :+= LittleEndianDecoder readDouble array
                case BYTE_ARRAY => data(col) :+= LittleEndianDecoder readString array
                case FIXED_LEN_BYTE_ARRAY => data(col) :+= LittleEndianDecoder readFixedLengthString(array, 8) //figure out length
              }

            j = j + 1
          }

          //inds(i) = inds(i) + j
        }

        //i = i + 1
      }
    }

    println(s"read in $test values")
    println(s"maxSkip = $maxSkip")

    data.toMap
  }

  def isParquetFile: Boolean = {
    val l = array length

    val footerLengthIndex = l - 4 - MAGIC.length

    val footerLength = {
      val x1 = array(footerLengthIndex) & 255
      val x2 = array(footerLengthIndex + 1) & 255
      val x3 = array(footerLengthIndex + 2) & 255
      val x4 = array(footerLengthIndex + 3) & 255

      if ((x1 | x2 | x3 | x4) < 0) throw new Error("Hit EOF early")

      (x4 << 24) + (x3 << 16) + (x2 << 8) + (x1 << 0)
    }

    val magic = new Array[Byte](MAGIC.length)

    for (i <- 0 until MAGIC.length) {
      magic(i) = array(footerLengthIndex + 4 + i)
    }

    array.pos = footerLengthIndex - footerLength

    magic.sameElements(MAGIC)
  }

  init()
}
