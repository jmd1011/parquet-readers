package main.scala

import java.io.{File, PrintWriter}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.io.api._
import org.apache.parquet.schema.{GroupType, MessageType, OriginalType, Type}

/**
  * Created by jdecker on 6/8/16.
  */
object Loader {
  def main(args: Array[String]): Unit = {
    val out = new PrintWriter(new File("./resources/customer_test.txt"))

    val p = new Path("./resources/customer.parquet")
    val reader = new ParquetReader[Record](p, new SimpleReadSupport())
    var value = reader.read()

    while (value != null) {
      out.println(value toString)
      value = reader.read()
    }

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
      //val record = new Record(Schema(schema.getFields.map(_.getName): _*), Fields(schema.getFields.map(_.getName): _*))
      val record = new Record()

      def Fields(s: String*): Fields = s.map(x => new RString(x, x.length)).toVector
      def Schema(s: String*): Schema = s.toVector

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

      override def end(): Unit = { if (parent != null) parent.record.add(name, record) }

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

  type Fields = Vector[RField]
  type Schema = Vector[String]

  abstract class RField {
    def print()

    def compare(o: RField): Boolean

    def hash: Long
  }

  case class RString(data: String, len: Int) extends RField {
    def print() = println(data)

    def compare(o: RField) = o match {
      case RString(data2, len2) => if (len != len2) false
      else {
        // TODO: we may or may not want to inline this (code bloat and icache considerations).
        var i = 0
        while (i < len && data.charAt(i) == data2.charAt(i)) {
          i += 1
        }
        i == len
      }
    }

    def hash = data.hashCode()
  }

  case class RInt(value: Int) extends RField {
    def print() = printf("%d", value)

    def compare(o: RField) = o match {
      case RInt(v2) => value == v2
    }

    def hash = value.asInstanceOf[Long]
  }

  class Record() {
    def add(name: String, value: Object): Unit = {
      val nv = new NameValue(name, value)
      values = values :+ nv
    }

    class NameValue(name: String, value: Object) {
      override def toString: String = s"$name: $value"
    }

    var values = List[NameValue]()
  }

//  class Record(schema: Schema, fields: Fields) {
//    def print(out: PrintWriter): Unit = {
//      for (i <- schema.indices) {
//        out.println(s"${schema.get(i)}: ${fields.get(i)}")
//      }
//
//      out.println()
//    }
//
//    def add(s: String, f: RField): Unit = {
//      fields.add(schema.indexOf(s), f)
//    }
//  }
}