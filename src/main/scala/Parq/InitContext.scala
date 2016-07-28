package main.scala.Parq

import org.apache.parquet.schema.MessageType

/**
  * Created by James on 7/28/2016.
  */
class InitContext(configuration: Configuration, keyValueMetaData: Map[String, Set[String]], fileSchema: MessageType) {
  def getFileSchema = fileSchema
}
