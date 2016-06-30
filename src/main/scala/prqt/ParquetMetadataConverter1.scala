package main.scala.prqt

import java.io.InputStream

import org.apache.parquet.format.Util.DefaultFileMetaDataConsumer
import org.apache.parquet.format.{FileMetaData, InterningProtocol}
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import parquet.org.apache.thrift.protocol.TCompactProtocol
import parquet.org.apache.thrift.transport.TIOStreamTransport

/**
  * Created by jdecker on 6/24/16.
  */
class ParquetMetadataConverter1 {
  def NO_FILTER = new NoFilter()

  def readParquetMetadata(from: InputStream, filter: MetadataFilter): ParquetMetadata = {
    var fileMetadata = filter.accept(new MetadataFilterVisitor[FileMetaData] {
      def readFileMetaData(f: InputStream, fmd: FileMetaData): FileMetaData = {
        def protocol() = {
          def protocol(ti: TIOStreamTransport) = {
            new InterningProtocol(new TCompactProtocol(ti))
          }

          protocol(new TIOStreamTransport(f))
        }

        fmd.read(protocol()) //side-effects. lovely.
        fmd
      }

      override def visit(noFilter: NoFilter): FileMetaData = readFileMetaData(from, new FileMetaData())

      override def visit(filter: SkipMetadataFilter): FileMetaData = {
        def readInner(defaultFileMetaDataConsumer: DefaultFileMetaDataConsumer): Unit = {

        }

        val md = new FileMetaData()
        readInner(new DefaultFileMetaDataConsumer(md))
      }

      override def visit(filter: RangeMetadataFilter): FileMetaData = ???
    })
  }
}

trait MetadataFilterVisitor[T] {
  def MetadataFilterVisitor() {}

  def visit(noFilter: NoFilter): T
  def visit(filter: SkipMetadataFilter): T
  def visit (filter: RangeMetadataFilter): T
}

abstract class MetadataFilter {
  def accept[T](visitor: MetadataFilterVisitor[T])
}

class NoFilter extends MetadataFilter {
  override def accept[T](visitor: MetadataFilterVisitor[T]): T = visitor.visit(this)
  override def toString = "NO_FILTER"
}

class SkipMetadataFilter extends MetadataFilter {
  override def accept[T](visitor: MetadataFilterVisitor[T]): T = visitor.visit(this)
  override def toString = "SKIP_ROW_GROUPS"
}

class RangeMetadataFilter(startOffset: Long, endOffset: Long) extends MetadataFilter {
  override def accept[T](visitor: MetadataFilterVisitor[T]): T = visitor.visit(this)
  def contains(n: Long): Boolean = n >= startOffset && n <= endOffset
}