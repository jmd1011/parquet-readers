package main.scala.Parq

/**
  * Created by James on 8/1/2016.
  */
abstract class RecordReader[T] {
  def read(): T
  def shouldSkipCurrentRecord(): Boolean = false
}
