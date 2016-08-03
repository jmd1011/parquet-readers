package main.scala.Parq

import org.apache.parquet.column.page.{PageReadStore, PageReader}
import org.apache.parquet.schema.MessageType

/**
  * Created by James on 8/1/2016.
  */
class ColumnReadStoreImpl(val pageReadStore: PageReadStore, val recordConverter: GroupConverter, schema: MessageType) extends ColumnReadStore {
  override def getColumnReader(columnDescriptor: ColumnDescriptor): ColumnReader = ???
//  private def newMemColumnReader(path: ColumnDescriptor, pageReader: PageReader) = {
//    val converter: PrimitiveConverter = this.getPrimitiveConverter(path)
//    new ColumnReaderImpl(path, pageReader, converter)
//  }

  private def getPrimitiveConverter(path: ColumnDescriptor): PrimitiveConverter = ???
}
