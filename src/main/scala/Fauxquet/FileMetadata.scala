package main.scala.Fauxquet

/**
  * Created by james on 8/5/16.
  */
class FileMetadata() extends Fauxquetable {
  var numRows: Long = _
  var version: Int = _
  var createdBy: String = _
  var rowGroups: List[RowGroup] = _

  var schema: Vector[SchemaElement] = Vector[SchemaElement]()

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, 1) => version = FauxquetDecoder readI32 arr
    case TField(_, 10, 3) => numRows = FauxquetDecoder readI64 arr
    case TField(_, 11, 6) => createdBy = FauxquetDecoder readString arr
    case TField(_, 15, x) => x match {
      case 2 =>
        val schema = FauxquetDecoder readListBegin arr
        for (i <- 0 until schema.size) this.schema :+= {
          val se = new SchemaElement
          se read arr
          se
        }
      case 4 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) rowGroups ::= {
          val rg = new RowGroup
          rg read arr
          rg
        }
      case 5 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) keyValueMetadata ::= {
          val kv = new KeyValue
          kv read arr
          kv
        }
      case _ => FauxquetDecoder skip(arr, field Type)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  //TODO
  def write(): Unit = ???

  def validate(): Unit = {
    if (schema == null || schema.isEmpty) throw new Error("Did not find schema in Parquet file.")
    if (version == -1) throw new Error("File Metadata version was not found in file.")
    if (numRows == -1) throw new Error("File Metadata numRows was not found in file.")
  }
}
