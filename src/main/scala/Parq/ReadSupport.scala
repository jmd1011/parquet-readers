package main.scala.Parq

/**
  * Created by James on 7/7/2016.
  */
abstract class ReadSupport[T] {
  val PARQUET_READ_SCHEMA = "parquet.read.schema"
  //val reader = Inter

  def prepareForRead(configuration: Configuration, keyValueMetaData: Map[String, String], fileSchema: MessageType, readContext: ReadContext): RecordMaterializer[T]
  def init(context: InitContext): ReadContext = new ReadContext(context fileSchema)

//  def read: T = {
//
//  }
}

class InitContext(val configuration: Configuration, val keyValueMetaData: Map[String, Set[String]], val fileSchema: MessageType)

class ReadContext(val requestedSchema: MessageType, val readSupportMetadata: Map[String, String]) {
  def this(requestedSchema: MessageType) = this(requestedSchema, null)

  if (requestedSchema == null) throw new Error("ReadContext.requestedSchema was null")
}
