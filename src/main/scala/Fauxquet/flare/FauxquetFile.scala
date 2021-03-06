package main.scala.Fauxquet.flare

import main.scala.Fauxquet.FauxquetObjs._
import main.scala.Fauxquet.flare.api.WriteSupport
import main.scala.Fauxquet.flare.metadata.FauxquetMetadataConverter
import main.scala.Fauxquet.schema.{BOOLEAN => _, DOUBLE => _, FIXED_LEN_BYTE_ARRAY => _, FLOAT => _, INT32 => _, INT64 => _, INT96 => _, _}

/**
  * Created by james on 8/5/16.
  */
class FauxquetFile() {
  type Schema = Vector[String]
  type Fields = Vector[String]

  case class Record(fields: Fields, schema: Schema) {
    def apply(key: String): String = fields(schema indexOf key)
    def apply(keys: Schema): Fields = keys.map(this apply _)
  }

  def Schema(schema: List[String]) = schema.toVector

  var data: Map[String, Vector[Any]] = Map[String, Vector[Any]]() //need to change this when dealing with Record
  var schema = Vector[SchemaElement]()
  var mtSchema: MessageType =_
  var fields: Fields = _
  var fauxquetMetadata: FauxquetMetadata = _

  def read(file: String) = {
    val fauxquetReader = new FauxquetReader(file)
    data = fauxquetReader.read()
    schema = fauxquetReader.fileMetaData.schema
    fauxquetMetadata = FauxquetMetadataConverter.fromFileMetadata(fauxquetReader.fileMetaData, schema.toList)
    mtSchema = FauxquetMetadataConverter.fromFauxquetSchema(schema.toList)
  }

  def write(file: String, schema: MessageType)(value: Map[Long, Map[String, String]]): Unit = {
    val fauxquetWriter = new FauxquetWriter(file, new WriteSupport(schema))

  }

  def write(file: String, schema: MessageType) = {
    val fauxquetWriter = new FauxquetWriter(file, new WriteSupport(schema))

    var test = Map[Long, Map[String, String]]()

    for (i <- data("nation_key").indices) {
      var m = Map[String, String]()

      for (key <- data.keySet) {
        m += (key -> data(key)(i).toString)
      }

      test += (i.asInstanceOf[Long] -> m)
    }

    fauxquetWriter.write(test)
    fauxquetWriter.close()
  }
}
