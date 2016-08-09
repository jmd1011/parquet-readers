package main.scala.Fauxquet

/**
  * Created by james on 8/5/16.
  */
class FileMetadata() extends Fauxquetable {
  var numRows: Long = _
  var version: Int = _
  var createdBy: String = _
  var schema: Vector[String] = Vector[String]()
  var keyValueMetadata: List[KeyValue] = List[KeyValue]()


  def validate(): Unit = {
    if (schema == null) throw new Error("Did not find schema in Parquet file.")
  }

  def read(arr: SeekableArray[Byte]): Unit = {
    FauxquetDecoder readStructBegin()

    var keepGoing = true

    while (keepGoing) {
      val field = FauxquetDecoder readFieldBegin arr

      if (field.Type == 0) { //means we've finished reading the file metadata
        FauxquetDecoder readStructEnd(field id)

        if (version == -1) throw new Error("File Metadata version was not found in file.")
        if (numRows == -1) throw new Error("File Metadata numRows was not found in file.")
        validate()

        keepGoing = false
      } else field match {
        case TField(_, 8, 1) => version = FauxquetDecoder readI32 arr
        case TField(_, 10, 3) => numRows = FauxquetDecoder readI64 arr
        case TField(_, 11, 6) => createdBy = FauxquetDecoder readString arr
        case TField(_, 15, x) => x match {
          case 2 =>
            val schema = FauxquetDecoder readListBegin arr
            for (i <- 0 until schema.size) this.schema :+= FauxquetDecoder.readSchemaItem(arr)
          case 4 => ???
          case 5 =>
            val list = FauxquetDecoder readListBegin arr
            for (i <- 0 until list.size) keyValueMetadata ::= { val kv = new KeyValue; kv read arr; kv}
          case _ => FauxquetDecoder skip(arr, field Type)
        }
        case _ => FauxquetDecoder skip(arr, field Type)
      }
    }
  }

  def write(): Unit = ???
}
