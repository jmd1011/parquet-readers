package main.scala.Fauxquet.FauxquetObjs.ColumnChunkMetadata
import main.scala.Fauxquet.FauxquetObjs.Encoding
import main.scala.Fauxquet.flare.metadata.ColumnPath
import main.scala.Fauxquet.schema.{INT64, PrimitiveTypeName}

/**
  * Created by james on 1/27/17.
  */
class LongColumnChunkMetadata extends ColumnChunkMetadata {
  override def dictionaryPageOffset: Long = ???

  override def firstDataPageOffset: Long = ???

  override def valueCount: Long = ???

  override def totalSize: Long = ???

  override def totalUncompressedSize: Long = ???

  override val Type: PrimitiveTypeName = INT64
  override var encodings: List[Encoding] = _
  override val path: ColumnPath = null
}
