package main.scala.Fauxquet.FauxquetObjs.ColumnChunkMetadata

/**
  * Created by james on 1/27/17.
  */
class LongColumnChunkMetadata extends ColumnChunkMetadata {
  override def dictionaryPageOffset: Long = ???

  override def firstDataPageOffset: Long = ???

  override def valueCount: Long = ???

  override def totalSize: Long = ???

  override def totalUncompressedSize: Long = ???
}
