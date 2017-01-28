package main.scala.Fauxquet.FauxquetObjs.ColumnChunkMetadata
import main.scala.Fauxquet.FauxquetObjs.{Encoding, TType}

/**
  * Created by james on 1/27/17.
  */
class LongColumnChunkMetadata extends ColumnChunkMetadata {
  override def dictionaryPageOffset: Long = ???

  override def firstDataPageOffset: Long = ???

  override def valueCount: Long = ???

  override def totalSize: Long = ???

  override def totalUncompressedSize: Long = ???

  override var Type: TType = _
  override var encodings: List[Encoding] = _
  override var path: Vector[String] = _
}
