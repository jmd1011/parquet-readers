package main.scala.Fauxquet.FauxquetObjs.ColumnChunkMetadata

/**
  * Created by james on 1/27/17.
  */
abstract class ColumnChunkMetadata {
  def startingPos: Long = {
    val dpo = dictionaryPageOffset
    val fdpo = firstDataPageOffset

    if (dpo > 0 && dpo < fdpo) dpo
    else fdpo
  }

  def get(firstDataPageOffset: Long, dictionaryPageOffset: Long, valueCount: Long, totalSize: Long, totalUncompressedSize: Long) =
    new IntColumnChunkMetadata(firstDataPageOffset, dictionaryPageOffset, valueCount, totalSize, totalUncompressedSize) //for testing purposes

  def dictionaryPageOffset: Long
  def firstDataPageOffset: Long
  def valueCount: Long
  def totalSize: Long
  def totalUncompressedSize: Long
}

/**
  * ONLY USED FOR get
  */
object ColumnChunkMetadataManager extends ColumnChunkMetadata {
  override def dictionaryPageOffset: Long = ???

  override def firstDataPageOffset: Long = ???

  override def valueCount: Long = ???

  override def totalSize: Long = ???

  override def totalUncompressedSize: Long = ???
}