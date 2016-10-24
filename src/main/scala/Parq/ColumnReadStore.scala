package main.scala.Parq

/**
  * Created by James on 8/1/2016.
  */
trait ColumnReadStore {
  def getColumnReader(columnDescriptor: ColumnDescriptor): ColumnReader
}
