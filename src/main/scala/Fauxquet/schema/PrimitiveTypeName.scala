package main.scala.Fauxquet.schema

import main.scala.Fauxquet.column.ColumnReader
import main.scala.Fauxquet.io.api.RecordConsumer

/**
  * Created by james on 1/28/17.
  */
trait PrimitiveTypeName {
  val name: String
  //type scalaType = T

  //def addValueToRecordConsumer(recordConsumer: RecordConsumer, columnReader: ColumnReader) //TODO: Figure out if we actually need this, think not
}
