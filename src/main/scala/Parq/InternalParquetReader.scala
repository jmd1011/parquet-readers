package main.scala.Parq

import main.scala.prqt.ParquetFileReader1
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData}
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.MessageType

import scala.collection.JavaConversions._

/**
  * Created by James on 7/28/2016.
  */
class InternalParquetReader[T](readSupport: ReadSupport[T]) {
  var columnIOFactory: ColumnIOFactory = _
  var reader: ParquetFileReader1 = _

  def initialize(fileSchema: MessageType, parquetFileMetadata: FileMetaData, file: Path, blocks: List[BlockMetaData], configuration: Configuration): Unit = {
    def toSetMultiMap[K, V](map: Map[K, V]): Map[K, Set[V]] = {
      def toSetMultiMap0(map: Map[K, V], acc: Map[K, Set[V]]): Map[K, Set[V]] = {
        if (map isEmpty) acc
        else toSetMultiMap0(map.tail, acc + (map.head._1 -> Set[V](map.head._2)))
      }

      toSetMultiMap0(map, Map[K, Set[V]]())
    }

    val fileMetadata = parquetFileMetadata.getKeyValueMetaData
    val readContext = readSupport.init(new InitContext(configuration, toSetMultiMap(fileMetadata.toMap), fileSchema))
    columnIOFactory = new ColumnIOFactory(parquetFileMetadata getCreatedBy)
    val columns = readContext.getRequestedSchema.getColumns.toList
    reader = new ParquetFileReader1(configuration, parquetFileMetadata, file, blocks, columns)
  }
}