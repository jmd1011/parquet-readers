package main.scala

import java.io.PrintWriter
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.io.api._
import org.apache.parquet.schema.{GroupType, MessageType, OriginalType, Type}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by jdecker on 6/8/16.
  */
object Loader {
  def main(args: Array[String]): Unit = {
    //val out = new PrintWriter(new java.io.File("./resources/customer_test.txt"))
    val out = new PrintWriter(System.out)

    val p = new Path("./resources/customer.parquet")
    val reader = new ParquetReader[Record](p, new SimpleReadSupport)
    var value = reader.read()
    var lValue = value

    while (value != null) {
      value.print(out)
      lValue = value
      value = reader.read()
    }


    lValue.print(out)

    reader.close()
  }

  class SimpleReadSupport extends ReadSupport[Record] {
    override def prepareForRead(configuration: Configuration, keyValueMetaData: util.Map[String, String], fileSchema: MessageType, readContext: ReadContext): RecordMaterializer[Record] = new SimpleRecordMaterializer(fileSchema)
    override def init(context: InitContext): ReadContext = new ReadContext(context.getFileSchema)
  }

  class SimpleRecordMaterializer(schema: MessageType) extends RecordMaterializer[Record] {
    val root = new SimpleRecordConverter(schema, null, null)

    override def getRootConverter: GroupConverter = root

    override def getCurrentRecord: Record = root.record

    class SimpleRecordConverter(schema: GroupType, name: String, parent: SimpleRecordConverter) extends GroupConverter {
      val converters = new Array[Converter](schema.getFieldCount)
      val record = new Record()

      def createConverters() = {
        def createConverter(field: Type): Converter = {
          class StringConverter(name: String) extends SimplePrimitiveConverter(name)

          if (field.isPrimitive) {
            val originalType = field.getOriginalType

            if (originalType != null && originalType == OriginalType.UTF8) {
              new StringConverter(field.getName)
            } else {
              new SimplePrimitiveConverter(field.getName)
            }
          } else {
            new SimpleRecordConverter(field.asGroupType(), field.getName, this)
          }
        }

        for (i <- 0 until schema.getFieldCount) {
          converters(i) = createConverter(schema.getFields.get(i))
        }
      }

      override def getConverter(i: Int): Converter = converters(i)

      override def end(): Unit = {}

      override def start(): Unit = {}

      class SimplePrimitiveConverter(name: String) extends PrimitiveConverter {
        override def addBinary(value: Binary): Unit = {
          record.add(name, new String(value.getBytes))
        }

        override def addFloat(value: Float): Unit = record.add(name, float2Float(value))
        override def addDouble(value: Double): Unit = record.add(name, double2Double(value))
        override def addInt(value: Int): Unit = record.add(name, int2Integer(value))
        override def addBoolean(value: Boolean): Unit = record.add(name, boolean2Boolean(value))
        override def addLong(value: Long): Unit = record.add(name, long2Long(value))
      }

      createConverters()
    }
  }

  class Record() {
    var map: mutable.Map[String, ListBuffer[Object]] = new mutable.HashMap[String, ListBuffer[Object]]()

    def add(name: String, value: Object): Unit = {
      if (!map.contains(name)) {
        map += (name -> new ListBuffer[Object]())
      }

      map(name) += value
    }

    def print(out: PrintWriter): Unit = {
      var max = -1

      for (key <- map) max = math.max(key._2.size, max)

      for (i <- 0 until max) {
        out.println(s"#${i + 1}:")

        for (key <- map) {
          out.println(s"${key._1}: ${key._2(i)}")
        }

        out.println()
      }
    }

    override def toString: String = {
      map toString
    }

    class NameValue(name: String, value: Object) {
      override def toString: String = s"$name: $value"
    }
  }
}