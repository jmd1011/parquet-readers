package main.scala.Fauxquet.schema

import main.scala.Fauxquet.column.ColumnReader
import main.scala.Fauxquet.io.api.RecordConsumer

/**
  * Created by james on 1/28/17.
  */
trait PrimitiveTypeName {
  val name: String
  //type scalaType

  def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader) //TODO: Figure out if we actually need this, think not
}

object INT32 extends PrimitiveTypeName {
  override val name: String = "Int"
  //override type scalaType = Int.type

  override def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader): Unit = recordConsumer.addInteger(columnReader.getInteger)
}

object INT64 extends PrimitiveTypeName {
  override val name: String = "Long"
  //override type scalaType = Long.type

  override def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader): Unit = recordConsumer.addLong(columnReader.getLong)
}

object BOOLEAN extends PrimitiveTypeName {
  override val name: String = "Boolean"
  //override type scalaType = Boolean.type

  override def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader): Unit = recordConsumer.addBool(columnReader.getBoolean)
}

object FLOAT extends PrimitiveTypeName {
  override val name: String = "Float"
  //override val scalaType = Float.type

  override def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader): Unit = recordConsumer.addFloat(columnReader.getFloat)
}

object DOUBLE extends PrimitiveTypeName {
  override val name: String = "Double"
  //override val scalaType = Double.type
  override def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader): Unit = recordConsumer.addDouble(columnReader.getDouble)
}

object INT96 extends PrimitiveTypeName {
  override val name: String = "Binary"
  //override val scalaType = String.type
  override def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader): Unit = recordConsumer.addBinary(columnReader.getBinary)
}

object FIXED_LEN_BYTE_ARRAY extends PrimitiveTypeName {
  override val name: String = "Binary"
  //override val scalaType = Byte.type
  override def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader): Unit = recordConsumer.addBinary(columnReader.getBinary)
}

object BINARY extends PrimitiveTypeName {
  override val name: String = "Binary"

  override def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader): Unit = recordConsumer.addBinary(columnReader.getBinary)
}