package main.scala.Fauxquet.FauxquetObjs.ColumnChunkMetadata

import main.scala.Fauxquet.FauxquetObjs.Encoding
import main.scala.Fauxquet.flare.metadata.ColumnPath
import main.scala.Fauxquet.schema.PrimitiveTypeName

/**
  * Created by james on 1/27/17.
  */
abstract class ColumnChunkMetadata {
  val Type: PrimitiveTypeName
  val path: ColumnPath

  def startingPos: Long = {
    val dpo = dictionaryPageOffset
    val fdpo = firstDataPageOffset

    if (dpo > 0 && dpo < fdpo) dpo
    else fdpo
  }

  def get(path: ColumnPath, primitiveTypeName: PrimitiveTypeName, firstDataPageOffset: Long, dictionaryPageOffset: Long, valueCount: Long, totalSize: Long, totalUncompressedSize: Long) =
    new IntColumnChunkMetadata(path, primitiveTypeName, firstDataPageOffset, math.max(dictionaryPageOffset, 0), valueCount, totalSize, totalUncompressedSize) //for testing purposes

  def dictionaryPageOffset: Long
  def firstDataPageOffset: Long
  def valueCount: Long
  def totalSize: Long
  def totalUncompressedSize: Long

  var encodings: List[Encoding]
  //var path: Vector[String]
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

  override val Type: PrimitiveTypeName = null
  override var encodings: List[Encoding] = Nil
  override val path: ColumnPath = null
}