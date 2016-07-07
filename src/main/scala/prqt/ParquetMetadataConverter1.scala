package main.scala.prqt

import java.io.InputStream

import scala.collection.JavaConversions._
import org.apache.parquet.format._
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.Types
import parquet.org.apache.thrift.protocol.TCompactProtocol
import parquet.org.apache.thrift.transport.TIOStreamTransport

/**
  * Created by jdecker on 6/24/16.
  */
class ParquetMetadataConverter1 {
  def NO_FILTER = new NoFilter()
  def SKIP_ROW_GROUPS = new SkipMetadataFilter()

  def readParquetMetadata(from: InputStream, filter: MetadataFilter): ParquetMetadata = {
    val fileMetadata = filter.accept(new MetadataFilterVisitor[FileMetaData] {
      def readFileMetaData(fmd: FileMetaData): FileMetaData = {
        def protocol() = {
          def protocol(ti: TIOStreamTransport) = {
            new InterningProtocol(new TCompactProtocol(ti))
          }

          protocol(new TIOStreamTransport(from))
        }

        fmd.read(protocol()) //side-effects. lovely.
        fmd
      }

      override def visit(noFilter: NoFilter): FileMetaData = org.apache.parquet.format.Util.readFileMetaData(from) //readFileMetaData(new FileMetaData())

      override def visit(filter: SkipMetadataFilter): FileMetaData = {
        org.apache.parquet.format.Util.readFileMetaData(from, true)
      }

      def getOffset(columnChunk: ColumnChunk): Long = {
        val md = columnChunk.getMeta_data
        val offset = md.getData_page_offset

        if (md.isSetDictionary_page_offset && offset > md.getDictionary_page_offset) md.getDictionary_page_offset
        else offset
      }

      def getOffset(rowGroup: RowGroup): Long = getOffset(rowGroup.getColumns.get(0))

      override def visit(filter: RangeMetadataFilter): FileMetaData = {
        def filter(metaData: FileMetaData, filter: RangeMetadataFilter) = {
          val rowGroups = metaData getRow_groups
          val nRowGroups = new java.util.LinkedList[RowGroup]()

          for (rGroup: RowGroup <- rowGroups) {
            var totalSize: Long = 0
            val cols = rGroup.getColumns
            val startI: Long = getOffset(cols.get(0))

            for (col: ColumnChunk <- cols) {
              totalSize += col.getMeta_data.getTotal_compressed_size
            }

            val mid = startI + totalSize / 2 //TODO: Check that this is correct

            if (filter contains mid) {
              nRowGroups.add(rGroup)
            }
          }

          metaData setRow_groups nRowGroups
          metaData
        }

        org.apache.parquet.format.Util.readFileMetaData(from)
      }

      override def visit(filter: OffsetMetadataFilter): FileMetaData = {
        def filterFileMetaDataByStart(metaData: FileMetaData, filter: OffsetMetadataFilter) = {
          val rowGroups = metaData getRow_groups
          val nRowGroups = new java.util.LinkedList[RowGroup]()

          for (rGroup: RowGroup <- rowGroups) {
            val startI: Long = getOffset(rGroup.getColumns.get(0))

            if (filter contains startI) {
              nRowGroups.add(rGroup)
            }
          }

          metaData setRow_groups nRowGroups
          metaData
        }

        filterFileMetaDataByStart(org.apache.parquet.format.Util.readFileMetaData(from), filter)
      }
    })

    def fromParquetMetaData(fileMetaData: FileMetaData): ParquetMetadata = {

    }

    def fromParquetSchema(schema: List[SchemaElement]) = {
      def buildChildren(builder: Types.GroupBuilder, schema: Iterator[SchemaElement], childrenCount: Int): Unit = {
        def getPrimitive(t: Type) {
          t match {
            case 
          }
        }

        for (i <- 0 until childrenCount) {
          val elem = schema next()

          val childBuilder: Types.Builder = {
            if (elem.`type` != null) {
              val primitiveBuilder = builder primitive(getPrimitive(elem `type`), fromParquetRepetition(elem repetition_type))

              if (elem isSetType_length) primitiveBuilder length (elem type_length)
              if (elem isSetPrecision) primitiveBuilder precision (elem precision)
              if (elem isSetScale) primitiveBuilder scale (elem scale)

              primitiveBuilder
            }
            else {
              val c: Types.GroupBuilder = builder.group(fromParquetRepetition(elem repetition_type))
              buildChildren(c, schema, elem num_children)

              c
            }
          }

          if (elem isSetConverted_type) childBuilder as getOriginalType(elem converted_type)
          if (elem isSetField_id) childBuilder id elem.field_id
        }
      }

      val it = schema iterator
      val root = it next
      val builder = Types.buildMessage()

    }

    fromParquetMetaData(fileMetadata)
  }
}

trait MetadataFilterVisitor[T] {
  def MetadataFilterVisitor() {}

  def visit(noFilter: NoFilter): T
  def visit(filter: SkipMetadataFilter): T
  def visit(filter: RangeMetadataFilter): T
  def visit(filter: OffsetMetadataFilter): T
}

abstract class MetadataFilter {
  def accept[T](visitor: MetadataFilterVisitor[T]): T
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

class OffsetMetadataFilter(offsets: Set[Long]) extends MetadataFilter {
  override def accept[T](visitor: MetadataFilterVisitor[T]): T = visitor.visit(this)
  def contains(offset: Long) = offsets.contains(offset)
}