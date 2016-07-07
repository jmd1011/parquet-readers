package main.scala.Parq

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.schema.MessageType

/**
  * Created by James on 7/7/2016.
  */
abstract class ReadSupport[T] {
  val PARQUET_READ_SCHEMA = "parquet.read.schema"
  //val reader = Inter

  def prepareForRead(configuration: Configuration, keyValueMetaData: util.Map[String, String], fileSchema: MessageType, readContext: ReadContext): RecordMaterializer[T]
  def init(context: InitContext): ReadContext = new ReadContext(context getFileSchema)

//  def read: T = {
//
//  }
}
