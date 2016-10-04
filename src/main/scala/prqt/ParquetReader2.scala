package main.scala.prqt

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.impl.ColumnReadStoreImpl
import org.apache.parquet.column.page.{Page, PageReadStore}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.io.api._
import org.apache.parquet.schema._

import scala.collection.JavaConversions._

/**
  * Created by jdecker on 6/21/16.
  */
object ParquetReader2 {
//  def Schema(schema: String*): Schema = schema.toVector
//
  def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    //println("Elapsed time: " + (t1 - t0) + "ns")
    //result
    t1 - t0
  }

  def main(args: Array[String]) {
    var sum = 0L

    for (i <- 0 until 30) {
      val t = time {
        val p = new Path("./resources/customer.parquet")
        val parquetMetadata = ParquetFileReader.readFooter(new Configuration(), p)
        val test = ParquetFileReader.readFooter(new Configuration(), p)
        val schema = parquetMetadata.getFileMetaData.getSchema

        dump(parquetMetadata, schema, p)
      }

      sum += t
    }

//    val t = time {
//      val p = new Path("./resources/customer.parquet")
//      val parquetMetadata = ParquetFileReader.readFooter(new Configuration(), p)
//      val test = ParquetFileReader.readFooter(new Configuration(), p)
//      val schema = parquetMetadata.getFileMetaData.getSchema
//
//      dump(parquetMetadata, schema, p)
//    }

    println(sum)
  }

  def dump(meta: ParquetMetadata, schema: MessageType, path: Path) = {
    val records: Array[Array[Any]] = new Array[Array[Any]](150000)
    var outerI = 0
    var arrIter = 0

    val configuration = new Configuration()
    val blocks = meta.getBlocks
    val columns = schema.getColumns
    val rule = "-----------------------------------"

    def dumpMetaData(): Unit = {
      //println(s"in dumpMetaData, about to loop i <- 0 until blocks.size() { blocks.size() = ${blocks.size()} }")
      for (i <- 0 until blocks.size()) {
        //println(s"i = $i")

//        if (i != 0) println()
//        println(s"Row Group $i\n")
//        println(rule)

        val ccmds = blocks.get(i).getColumns

        //println("starting printStuff(ccmds)")
        //printStuff(ccmds)
        //println("finished printStuff(ccmds)")

        //println(s"creating ParquetFileReader for i = $i")
        val freader = new ParquetFileReader(configuration, path, blocks toList, columns toList)

        var store = freader.readNextRowGroup()
        while (store != null) {
          //println("store was not null")

          for (i <- 0 until columns.size()) {
            //println("about to dump the metadata (inner call to dump(store, columns.get(i)))")
            dump(store, columns.get(i))
          }

          store = freader.readNextRowGroup()
        }

        //println("closing freader")
        freader.close()
      }

      //println("exiting dumpMetaData")
    }

    def dumpData(): Unit = {
      def dump(crstore: ColumnReadStoreImpl, column: ColumnDescriptor, page: Long, total: Long, offset: Long) = {
        val maxDef = column.getMaxDefinitionLevel
        val creader = crstore.getColumnReader(column)
        //print(s"*** row group $page of $total, values $offset to ${offset + creader.getTotalValueCount - 1}")
        //println()

        val nextValue: () => Any = column.getType match {
            case PrimitiveType.PrimitiveTypeName.BINARY => () => new String(creader.getBinary.getBytes)
            case PrimitiveType.PrimitiveTypeName.BOOLEAN => () => creader.getBoolean
            case PrimitiveType.PrimitiveTypeName.DOUBLE => () => creader.getDouble
            case PrimitiveType.PrimitiveTypeName.FLOAT => () => creader.getFloat
            case PrimitiveType.PrimitiveTypeName.INT32 => () => creader.getInteger
            case PrimitiveType.PrimitiveTypeName.INT64 => creader.getLong
            case PrimitiveType.PrimitiveTypeName.INT96 => () => new String(creader.getBinary.getBytes)
            case PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => () => new String(creader.getBinary.getBytes)
          }

        for (i <- 0L until creader.getTotalValueCount) {
          val curDef = creader.getCurrentDefinitionLevel

          if (curDef == maxDef) {
            if (outerI == 0) records(arrIter) = new Array[Any](columns.size())

            records(arrIter)(outerI) = nextValue()
            //nextValue()
            //print(nextValue())
          } else {
            //print("<null>")
          }

          //println()
          creader.consume()
          arrIter += 1
        }
      }

      class DumpGroupConverter extends GroupConverter {
        override def getConverter(i: Int): Converter = {
          class DumpConverter extends PrimitiveConverter {
            override def isPrimitive = true
            override def asGroupConverter() = new DumpGroupConverter()
          }

          new DumpConverter()
        }

        override def end(): Unit = {}

        override def start(): Unit = {}
      }


      for (i <- 0 until columns.size()) {
        val column = columns.get(i)
        //println()
        //println(s"${column.getType} ${column.getPath.mkString(".")}")
        //println(rule)

        var page = 1L
        val total = blocks.size()
        //println(s"total = $total")
        var offset = 1L

        val freader = new ParquetFileReader(configuration, path, blocks, columns)
        var store = freader.readNextRowGroup()

        while (store != null) {
          val crstore = new ColumnReadStoreImpl(store, new DumpGroupConverter(), schema, "James Decker")
          dump(crstore, column, page, total, offset)

          page += 1
          offset += store.getRowCount
          store = freader.readNextRowGroup()
        }

        freader.close()

        outerI = outerI + 1
        arrIter = 0
      }

      //println("test")
    }

    def dump(store: PageReadStore, column: ColumnDescriptor): Unit = {
      val reader = store.getPageReader(column)
      val rule = "-----------------------------------"

      val vc = reader.getTotalValueCount
      val rmax = column.getMaxRepetitionLevel
      val dmax = column.getMaxDefinitionLevel
//      print(s"${column.getPath.mkString(".")} TV=$vc RL=$rmax DL=$dmax")

      val dict = reader.readDictionaryPage()

//      if (dict != null) {
//        //print(s" DS:${dict.getDictionarySize} DE:${dict.getEncoding}")
//      }

      //println()
      //println(rule)

      val page: Page = reader.readPage()

//      var i = 0
//
//      while (page != null) {
//        print(s"page $i: DLE:$page")
//        i += 1
//      }
    }

    def printStuff(ccmds: util.List[ColumnChunkMetaData]): Unit = {
      val chunks: util.Map[String, Object] = new util.LinkedHashMap[String, Object]()

      //println(s"starting i <- 0 until ccmds.size() where ccmds.size() = ${ccmds.size()}")
      for (i <- 0 until ccmds.size()) {
        //println()
        //println(s"i = $i")

        val cmeta = ccmds.get(i)
        val path = cmeta.getPath.toArray

        var current = chunks

        //println("about to loop over path.length")
        for (j <- 0 until path.length) {
          //println(s"j = $j")

          val next = path(j)

          if (!current.containsKey(next)) {
            current.put(next, new util.LinkedHashMap[String, Object]())
          }

          current = current.get(next).asInstanceOf[util.LinkedHashMap[String, Object]]
        }

        current.put(path(path.length - 1), cmeta)
      }

      //println("calling showColumnChunkDetails(chunks, 0)")
      showColumnChunkDetails(chunks, 0)
      //println("made it out of showColumnChunkDetails")

      //println("exiting printStuff")
    }

    def showColumnChunkDetails(current: util.Map[String, Object], depth: Int): Unit = {
      for (entry: util.Map.Entry[String, Object] <- current.entrySet()) {
        val className = entry.getKey
        val value: Object = entry.getValue

        if (value.isInstanceOf[util.Map[String, Object]]) {
          //println(className + ": ")
          showColumnChunkDetails(value.asInstanceOf[util.Map[String, Object]], depth + 1)
        } else {
          //print(className + ": ")
          showDetails(value.asInstanceOf[ColumnChunkMetaData], className = false)
        }
      }
    }

    def showDetails(meta: ColumnChunkMetaData, className: Boolean) = {
      val doff = meta.getDictionaryPageOffset
      val foff = meta.getFirstDataPageOffset
      val tsize = meta.getTotalSize
      val usize = meta.getTotalUncompressedSize
      val count = meta.getValueCount
      val ratio = usize / tsize.asInstanceOf[Double]
      val encodings = meta.getEncodings.toArray.mkString(",")

//      if (className) {
//        print(meta.getPath.toArray.mkString(",") + ": ")
//      }

      //print(s" ${meta.getType} ${meta.getCodec} DO:$doff FPO:$foff SZ:$tsize/$usize/$ratio VC:$count")
//      if (!encodings.isEmpty) print(s" ENC:$encodings")
//      println()
//      println()
    }

    //dumpMetaData()
    dumpData()
  }
}
