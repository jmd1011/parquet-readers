package main.scala.prqt

import java.io.InputStream
import java.nio.charset.Charset

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ParquetMetadata}

/**
  * Created by jdecker on 6/24/16.
  */
class ParquetFileReader1 extends java.io.Closeable {
//  def this(configuration: Configuration, path: Path, blocks: List[BlockMetaData], columns: List[ColumnDescriptor]) = this()
//
//  val MAGIC = "PAR1".getBytes(Charset.forName("ASCII"))
//
//  val parquetMetadataConverter = new ParquetMetadataConverter1()
//
  override def close(): Unit = {}
//
//  def readFooter(configuration: Configuration, path: Path): ParquetMetadata = readFooter(configuration, path, parquetMetadataConverter.NO_FILTER)
//
//  def readFooter(configuration: Configuration, path: Path, metadataFilter: MetadataFilter): ParquetMetadata = {
//    val fileSystem = path.getFileSystem(configuration)
//    readFooter(configuration, fileSystem.getFileStatus(path), metadataFilter)
//  }
//
//  def readFooter(configuration: Configuration, fileStatus: FileStatus, metadataFilter: MetadataFilter): ParquetMetadata = {
//    val path = fileStatus.getPath
//    val fileSystem = path.getFileSystem(configuration)
//    val f = fileSystem.open(path)
//
//    val length = fileStatus.getLen
//    val FOOTER_LENGTH_SIZE = 4
//
//    if (length < MAGIC.length * 2 + FOOTER_LENGTH_SIZE) {
//      throw new Exception("This file is too small homie")
//    }
//
//    val footerLengthIndex = length - FOOTER_LENGTH_SIZE - MAGIC.length
//    f.seek(footerLengthIndex)
//
//    val footerLength = readIntLittleEndian(f)
//    val magic = new Array[Byte](MAGIC.length)
//    f.readFully(magic)
//
//    if (!MAGIC.eq(magic)) {
//      throw new Exception("Still not a Parquet file friendo. Missing the magic number.")
//    }
//
//    val footerIndex = footerLengthIndex - footerLength
//
//    if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
//      throw new Exception("This file is straight up corrupt.")
//    }
//
//    f.seek(footerIndex)
//
//    parquetMetadataConverter.readParquetMetadata(f, metadataFilter)
//  }
//
//  def readIntLittleEndian(in: InputStream): Int = {
//    val ch1 = in.read()
//    val ch2 = in.read()
//    val ch3 = in.read()
//    val ch4 = in.read()
//    if ((ch1 | ch2 | ch3 | ch4) < 0) {
//      throw new Exception("Hit the end of file brah!")
//    }
//
//    (ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1 << 0)
//  }
//
//  def readNextRowGroup() = ???
}