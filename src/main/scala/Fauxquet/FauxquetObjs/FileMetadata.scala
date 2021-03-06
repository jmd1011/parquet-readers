package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet._
import main.scala.Fauxquet.schema.MessageType

/**
  * Created by james on 8/5/16.
  */
class FileMetadata(var schem: MessageType = null, val extraMetaData: Map[String, String] = null, var createdBy: String = "Flare Team") extends Fauxquetable {
  var numRows: Long = _
  var version: Int = 1
  var rowGroups: List[RowGroup] = Nil

  var schema: Vector[SchemaElement] = Vector[SchemaElement]() //TODO: Fix this

  private val VERSION_FIELD_DESC = TField("version", 8, 1)
  private val SCHEMA_FIELD_DESC = TField("schema", 15, 2)
  private val NUM_ROWS_FIELD_DESC = TField("num_rows", 10, 3)
  private val ROW_GROUPS_FIELD_DESC = TField("row_groups", 15, 4)
  private val KEY_VALUE_METADATA_FIELD_DESC = TField("key_value_metadata", 15, 5)
  private val CREATED_BY_FIELD_DESC = TField("created_by", 11, 6)

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, 1) => version = FauxquetDecoder readI32 arr
    case TField(_, 10, 3) => numRows = FauxquetDecoder readI64 arr
    case TField(_, 11, 6) => createdBy = FauxquetDecoder readString arr
    case TField(_, 15, x) => x match {
      case 2 =>
        val schema = FauxquetDecoder readListBegin arr
        for (i <- 0 until schema.size) {
          val se = new SchemaElement
          se read arr
          if (se.name != null) this.schema :+= se
        }
      case 4 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) rowGroups :+= {
          val rg = new RowGroup
          rg read arr
          rg
        }
      case 5 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) keyValueMetadata :+= {
          val kv = new KeyValue
          kv read arr
          kv
        }
      case _ => FauxquetDecoder skip(arr, field Type)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  override def doWrite(): Unit = {
    def writeVersion(): Unit = {
      FauxquetEncoder writeFieldBegin VERSION_FIELD_DESC
      FauxquetEncoder writeI32 version
      FauxquetEncoder writeFieldEnd()
    }

    def writeSchema(): Unit = {
      FauxquetEncoder writeFieldBegin SCHEMA_FIELD_DESC
      FauxquetEncoder writeListBegin TList(12, schema size)

      for (se <- schema) {
        se.write(FauxquetEncoder.encoder)
      }

      FauxquetEncoder writeListEnd()
      FauxquetEncoder writeFieldEnd()
    }

    def writeNumRows(): Unit = {
      FauxquetEncoder writeFieldBegin NUM_ROWS_FIELD_DESC
      FauxquetEncoder writeI64 numRows
      FauxquetEncoder writeFieldEnd()
    }

    def writeRowGroups(): Unit = {
      FauxquetEncoder writeFieldBegin ROW_GROUPS_FIELD_DESC
      FauxquetEncoder writeListBegin TList(12, rowGroups size)

      for (rg <- rowGroups) {
        rg.write(FauxquetEncoder.encoder)
      }

      FauxquetEncoder writeListEnd()
      FauxquetEncoder writeFieldEnd()
    }

    def writeKeyValueMetadata(): Unit = {
      FauxquetEncoder writeFieldBegin KEY_VALUE_METADATA_FIELD_DESC
      FauxquetEncoder writeListBegin TList(12, keyValueMetadata size)

      for (kv <- keyValueMetadata) {
        kv.write(FauxquetEncoder.encoder)
      }

      FauxquetEncoder writeListEnd()
      FauxquetEncoder writeFieldEnd()
    }

    def writeCreatedBy(): Unit = {
      FauxquetEncoder writeFieldBegin CREATED_BY_FIELD_DESC
      FauxquetEncoder writeString createdBy
      FauxquetEncoder writeFieldEnd()
    }

    writeVersion()

    if (this.schema != null) {
      writeSchema()
    }

    writeNumRows()

    if (this.rowGroups != null && this.rowGroups != Nil) {
      writeRowGroups()
    }

    if (this.keyValueMetadata != null && this.keyValueMetadata != Nil) {
      writeKeyValueMetadata()
    }

    if (this.createdBy != null) {
      writeCreatedBy()
    }
  }

  def validate(): Unit = {
    if (schema == null || schema.isEmpty) throw new Error("Did not find schema in Parquet file.")
    if (version == -1) throw new Error("File Metadata version was not found in file.")
    if (numRows == -1) throw new Error("File Metadata numRows was not found in file.")
  }

  override def className: String = "FileMetadata"
}
