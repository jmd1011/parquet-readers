package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet.FauxquetObjs.ColumnChunkMetadata.ColumnChunkMetadata

/**
  * Created by james on 1/27/17.
  */
class BlockMetadata() {
  var path: String
  var rowCount: Long
  var totalBytesSize: Long

  var columns: List[ColumnChunkMetadata] = List[ColumnChunkMetadata]()

  def addColumn(column: ColumnChunkMetadata) = columns = columns :: column

  def startingPos: Long = columns.head.startingPos

  def compressedSize = (0L /: columns) (_ + _.totalSize)
}
