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
    //recordConsumer.startMessage()

    if (values.size != columns.size) {
      throw new Error("Incompatible schema")
    }

    for (i <- columns.indices) {
      val col = columns(i)
      val field = col.path.head
      val v = values(i)

      recordConsumer.startField(field, i)

      if (v.length > 0) {
        col.primitive match {
          case BOOLEAN => recordConsumer.addBool(v.toBoolean)
          case INT32 => recordConsumer.addInteger(v.toInt)
          case INT64 => recordConsumer.addLong(v.toLong)
          case BINARY => recordConsumer.addBinary(BinaryManager.fromString(v))
          case FLOAT => recordConsumer.addFloat(v.toFloat)
          case DOUBLE => recordConsumer.addDouble(v.toDouble)
          case _ => throw new Error("Unsupported column type")
        }

        recordConsumer.endField(field, i)
      }
    }

    //recordConsumer.endMessage()
  }

  def write(values: Map[String, String]): Unit = {
//    if (values.size != columns.size) {
//      throw new Error("Schema mismatch")
//    }

    recordConsumer.startMessage()

    if (values.keySet.size != columns.size) {
      throw new Error("Incompatible schema")
    }

    for (j <- columns.indices) {
      val col = columns(j)
      val field = col.path.head
      val v = values(field)

      recordConsumer.startField(field, j)

      if (v.length > 0) {
        col.primitive match {
          case BOOLEAN => recordConsumer.addBool(v.toBoolean)
          case INT32 => recordConsumer.addInteger(v.toInt)
          case INT64 => recordConsumer.addLong(v.toLong)
          case BINARY => recordConsumer.addBinary(BinaryManager.fromString(v))
          case FLOAT => recordConsumer.addFloat(v.toFloat)
          case DOUBLE => recordConsumer.addDouble(v.toDouble)
          case _ => throw new Error("Unsupported column type")
        }

        recordConsumer.endField(field, j)
      }
    }

    recordConsumer.endMessage()

    /* old
//    recordConsumer.startMessage()
//    for (i <- 0 until values.keySet.size) {
//      for (j <- columns.indices) {
//        val col = columns(j)
//        val v1 = values(i)
//        val v = v1(col.path(0))
//
//        //for ((k, v) <- v1) {
//        recordConsumer.startField(col.path(0), j)
//
////        if (v.length > 0) {
////          col.primitive match {
////            case BOOLEAN => recordConsumer.addBool(v.toBoolean)
////            case INT32 => recordConsumer.addInteger(v.toInt)
////            case INT64 => recordConsumer.addLong(v.toLong)
////            case BINARY => recordConsumer.addBinary(BinaryManager.fromString(v))
////            case FLOAT => recordConsumer.addFloat(v.toFloat)
////            case DOUBLE => recordConsumer.addDouble(v.toDouble)
////            case _ => throw new Error("Unsupported column type")
////          }
////        }
//
//        recordConsumer.endField(col.path(0), i)
//      }
//    }

//    for (i <- 0 until values.keySet.size) {
//      val col = columns(i)
//
//      val v1 = values(i)
//      val v = v1(col.path(0))
//
//      //for ((k, v) <- v1) {
//        recordConsumer.startField(col.path(0), i)
//
//        if (v.length > 0) {
//          col.primitive match {
//            case BOOLEAN => recordConsumer.addBool(v.toBoolean)
//            case INT32 => recordConsumer.addInteger(v.toInt)
//            case INT64 => recordConsumer.addLong(v.toLong)
//            case BINARY => recordConsumer.addBinary(BinaryManager.fromString(v))
//            case FLOAT => recordConsumer.addFloat(v.toFloat)
//            case DOUBLE => recordConsumer.addDouble(v.toDouble)
//            case _ => throw new Error("Unsupported column type")
//          }
//        }
//
//        recordConsumer.endField(col.path(0), i)
//      //}
//    }
//
//    recordConsumer.endMessage()
*/
  }

  val name = "Flare Project"
}
