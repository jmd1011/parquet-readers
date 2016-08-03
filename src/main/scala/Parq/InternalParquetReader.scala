package main.scala.Parq

import main.scala.prqt.ParquetFileReader1
import org.apache.parquet.hadoop.metadata.{BlockMetaData, FileMetaData}

import scala.collection.JavaConversions._

/**
  * Created by James on 7/28/2016.
  */
class InternalParquetReader[T](readSupport: ReadSupport[T]) {
  var columnIOFactory: ColumnIOFactory = _
  var reader: ParquetFileReader1 = _
  var recordConverter: RecordMaterializer[T] = _

  var requestedSchema: MessageType = _
  var fileSchema: MessageType = _

  var total: Long = 0L
  var current: Long = 0L
  var totalCountLoadedSoFar: Long = 0L

  var currentValue: T = _
  var recordReader: RecordReader[T] = _

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
    requestedSchema = readContext requestedSchema
    //this.fileSchema = fileSchema

    columnIOFactory = new ColumnIOFactory(parquetFileMetadata getCreatedBy)

    val columns = requestedSchema columns
    //reader = new ParquetFileReader1(configuration, parquetFileMetadata, file, blocks, columns)

    for (block <- blocks) {
      total += block.getRowCount
    }
  }

  def hasNextKeyValue: Boolean = {
    while (current < total) {
      checkRead()

      current += 1
      currentValue = recordReader.read()

      if (currentValue != null) true
    }

    false
  }

  def checkRead(): Boolean = true //{
//    if (current == totalCountLoadedSoFar) {
//      val pages = reader.readNextRowGroup()
//
//      if (pages == null) false
//
//      val columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema)
//      recordReader = columnIO.getRecordReader(pages, recordConverter, null)
//    }
//
//    true
//  }
}