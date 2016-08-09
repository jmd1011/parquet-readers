package main.scala.Fauxquet

/**
  * Created by james on 8/5/16.
  */

trait Fauxquetable {
  def read(arr: SeekableArray[Byte])
  def write()

  def validate()
}