package main.scala.Fauxquet.flare

import main.scala.Fauxquet.FauxquetObjs.SchemaElement

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
  var fields: Fields = _

  def read(file: String) = {
    val fauxquetReader = new FauxquetReader(file)
    data = fauxquetReader.read()
    schema = fauxquetReader.fileMetaData.schema
  }

  def write(file: String) = {
    val fauxquetWriter = new FauxquetWriter(file)
    fauxquetWriter.write(schema, data)
  }
}
