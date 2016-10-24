package main.scala.Parq

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.hadoop.{Footer, ParquetFileReader}

import scala.collection.JavaConversions._

/**
  * Created by James on 7/7/2016.
  */
class ParquetReader[T](file: Path, readSupport: ReadSupport[T]) extends java.io.Closeable {
  val configuration: Configuration = new Configuration()
//  lazy val footersIterator: Iterator[Footer] = {
//    val fs = file getFileSystem configuration
//    val statuses = fs.listStatus(file, HiddenFileFilter.INSTANCE)
//    val footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, statuses.toList, skipRowGroups = false)
//  }

  override def close(): Unit = ???


}
