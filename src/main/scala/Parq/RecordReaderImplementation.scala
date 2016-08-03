package main.scala.Parq

/**
  * Created by James on 8/1/2016.
  */
class RecordReaderImplementation[T](val root: MessageColumnIO, val recordMaterializer: RecordMaterializer[T], val validating: Boolean, val columnStore: ColumnReadStoreImpl) extends RecordReader[T] {
  override def read(): T = ???
}
