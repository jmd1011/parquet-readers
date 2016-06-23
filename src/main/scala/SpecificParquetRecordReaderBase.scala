package main.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.parquet.filter2.compat.{FilterCompat, RowGroupFilter}
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat, ParquetInputSplit}
import org.apache.parquet.hadoop.api.{InitContext, ReadSupport}
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ParquetMetadata}
import org.apache.parquet.hadoop.util.ConfigurationUtil
import org.apache.parquet.schema.{GroupType, MessageType}

import scala.collection.JavaConversions._
import scala.collection.immutable.HashSet

/**
  * Created by jdecker on 6/20/16.
  */
abstract class SpecificParquetRecordReaderBase[T] extends RecordReader[Void, T] {
  var fileSchema: MessageType = null
  var requestedSchema: MessageType = null
  var totalRowCount: Long = 0
  var reader: ParquetFileReader = null
  val path: Path = null

  override def getProgress: Float = 0F

  override def nextKeyValue(): Boolean = true

  override def getCurrentValue: T = ???

  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit = {
    val configuration: Configuration = taskAttemptContext.getConfiguration
    val split = inputSplit.asInstanceOf[ParquetInputSplit]
    val rowGroupOffsets: Array[Long] = split.getRowGroupOffsets
    var footer: ParquetMetadata = null// ParquetFileReader.readFooter(configuration, path, { if (rowGroupOffsets == null) ParquetMetadataConverter.NO_FILTER else  ParquetMetadataConverter.range(split.getStart, split.getEnd) })
    var blocks: List[BlockMetaData] = Nil

    if (rowGroupOffsets == null) {
      footer = ParquetFileReader.readFooter(configuration, path, ParquetMetadataConverter.range(split.getStart, split.getEnd))
      fileSchema = footer.getFileMetaData.getSchema
      val filter: FilterCompat.Filter = ParquetInputFormat.getFilter(configuration)
      blocks = RowGroupFilter.filterRowGroups(filter, footer.getBlocks, fileSchema).toList
    } else {
      footer = ParquetFileReader.readFooter(configuration, path, ParquetMetadataConverter.NO_FILTER)
      blocks = List[BlockMetaData]()

      for (block <- footer.getBlocks) {
        if (rowGroupOffsets contains block.getStartingPos) {
          blocks.add(block)
        }
      }

      if (blocks.size != rowGroupOffsets.length) {
        throw new Exception("This supposedly should never happen...")
      }
    }

    val fileMetadata:Map[String, String] = footer.getFileMetaData.getKeyValueMetaData.toMap[String, String]
    val readSupport = getReadSupportInstance(getReadSupportClass(configuration))
    val readContext = readSupport.init(new InitContext(taskAttemptContext.getConfiguration, toSetMultiMap(fileMetadata), fileSchema))

    requestedSchema = readContext.getRequestedSchema
    reader = new ParquetFileReader(configuration, path, blocks, requestedSchema.getColumns)
    totalRowCount += blocks.map(_.getRowCount).sum
  }

  def listDirectory(path: java.io.File): List[String] = {
    val result:List[String] = List[String]()

    if (path.isDirectory) {
      for (f <- path.listFiles()) {
        result addAll listDirectory(f)
      }
    } else {
      val c = path.getName.charAt(0)
      if (c != '.' && c != '_') {
        result add path.getAbsolutePath
      }
    }

    result
  }

  protected def initialize(p: String, columns: List[String]): Unit = {
    initialize(columns)
  }

  protected def initialize(columns: List[String]): Unit = {
    val configuration = new Configuration()
    configuration.set("spark.sql.parquet.binaryAsString", "false")
    configuration.set("spark.sql.parquet.int96AsTimestamp", "false")
    configuration.set("spark.sql.parquet.writeLegacyFormat", "false")

    val length: Long = path.getFileSystem(configuration).getFileStatus(path).getLen
    val footer = ParquetFileReader.readFooter(configuration, path, ParquetMetadataConverter.range(0, length))
    val blocks = footer.getBlocks
    fileSchema = footer.getFileMetaData.getSchema

    if (columns == null) {
      requestedSchema = fileSchema
    }

    reader = new ParquetFileReader(configuration, path, blocks, requestedSchema.getColumns)
    totalRowCount += blocks.map(_.getRowCount).sum

    //having issues with else for now
//    } else {
//      if (columns.nonEmpty) {
//        val builder: org.apache.parquet.schema.Types.MessageTypeBuilder = org.apache.parquet.schema.Types.buildMessage()
//
//        for (s <- columns) {
//          if (!fileSchema.containsField(s)) {
//            throw new Exception("This column doesn't exist, brah.")
//          }
//
//          //ugly hack due to Java/Scala interop issues :/
//          builder.addFields(fileSchema.asInstanceOf[GroupType].getType(s))
//        }
//
//
//      }
//    }
  }

  private def getReadSupportClass(configuration: Configuration): Class[_ <: ReadSupport[T]] = {
    ConfigurationUtil.getClassFromConfig(configuration, ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ReadSupport[T]]).asInstanceOf[Class[_ <: ReadSupport[T]]]
  }

  private def getReadSupportInstance(readSupportClass: Class[_ <: ReadSupport[T]]): ReadSupport[T] = {
    readSupportClass.getConstructor().newInstance()
  }

  private def toSetMultiMap[K, V](map: Map[K, V]): java.util.Map[K, java.util.Set[V]] = {
    val smm: Map[K, java.util.Set[V]] = Map[K, java.util.Set[V]]()

    for (entry <- map.entrySet()) {
      val set = new HashSet[V]
      set.add(entry.getValue)
      smm.add(entry.getKey -> set)
    }

    smm.asInstanceOf[java.util.Map[K, java.util.Set[V]]]
  }

  override def getCurrentKey: Void = null

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
      reader = null
    }
  }
}
