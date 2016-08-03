package main.scala.Parq

import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.io.{GroupColumnIO, PrimitiveColumnIO}

/**
  * Created by James on 7/28/2016.
  */
class MessageColumnIO { //extends GroupColumnIO {
  var leaves: List[PrimitiveColumnIO] = _

  //override def getType: MessageType = ???

  def getRecordReader[T](columns: PageReadStore, recordMaterializer: RecordMaterializer[T], filter: Filter): RecordReader[T] = {
    if (leaves isEmpty) new EmptyRecordReader(recordMaterializer)
    else filter.accept(new Visitor[RecordReader[T]] {
      override def visit(filterPredicateCompat: FilterPredicateCompat): RecordReader[T] = ??? //{
//        val predicate: FilterPredicate = filterPredicateCompat filterPredicate
//        new RecordReaderImplementation(MessageColumnIO.this, null, true, new ColumnReadStoreImpl(columns, null, MessageColumnIO.this.getType))
//      }


    })
  }
}
