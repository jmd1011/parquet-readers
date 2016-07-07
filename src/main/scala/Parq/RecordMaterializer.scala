package main.scala.Parq

/**
  * Created by James on 7/7/2016.
  */
abstract class RecordMaterializer[T] {
  def getCurrentRecord: T
  def skipCurrentRecord() = {}
  def getRootConverter(): GroupConverter
}
