package main.scala.Parq

/**
  * Created by James on 8/1/2016.
  */
class EmptyRecordReader[T](recordMaterializer: RecordMaterializer[T]) extends RecordReader[T] {
  val recordConsumer: GroupConverter = recordMaterializer.getRootConverter()
//  var recordMaterializer: RecordMaterializer[T] = _
//
//  def this(recordMaterializer: RecordMaterializer[T]) = {
//    this()
//    this.recordMaterializer = recordMaterializer
//    this.recordConsumer = recordMaterializer getRootConverter()
//  }

  override def read(): T = {
    this.recordConsumer.start()
    this.recordConsumer.end()

    this.recordMaterializer.getCurrentRecord
  }
}
