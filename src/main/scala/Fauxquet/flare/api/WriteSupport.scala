package main.scala.Fauxquet.flare.api

import main.scala.Fauxquet.column.ColumnDescriptor
import main.scala.Fauxquet.io.api.{BinaryManager, RecordConsumer}
import main.scala.Fauxquet.schema._

/**
  * Created by james on 1/30/17.
  * This is generic in Parquet. However, because we know the format of our data, we can make this much less complex
  * Note: This is basically just the CSVWriteSupport from parquet-compat
  */
class WriteSupport(val schema: MessageType) {
  var recordConsumer: RecordConsumer = _

  val columns: List[ColumnDescriptor] = schema.columns()

  class WriteContext(schema: MessageType, extraMetadata: Map[String, String]) { }

  def init(): WriteContext = new WriteContext(schema, Map[String, String]())

  def prepareForWrite(recordConsumer: RecordConsumer): Unit = this.recordConsumer = recordConsumer
  def write(values: List[String]): Unit = {
    if (values.size != columns.size) {
      throw new Error("Schema mismatch")
    }

    for (i <- values.indices) {
      val field = columns(i).path(0)

      recordConsumer.startField(field, i)

      val v = values(i)

      if (v.length > 0) {
        columns(i).primitive match {
          case BOOLEAN => recordConsumer.addBool(v.toBoolean)
          case INT32 => recordConsumer.addInteger(v.toInt)
          case INT64 => recordConsumer.addLong(v.toLong)
          case BINARY => recordConsumer.addBinary(BinaryManager.fromString(v))
          case FLOAT => recordConsumer.addFloat(v.toFloat)
          case DOUBLE => recordConsumer.addDouble(v.toDouble)
          case _ => throw new Error("Unsupported column type")
        }
      }

      recordConsumer.endField(field, i)
    }

    recordConsumer.endMessage()
  }

  val name = "Flare Project"
}
