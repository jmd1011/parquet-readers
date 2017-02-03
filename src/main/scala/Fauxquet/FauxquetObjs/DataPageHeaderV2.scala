package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet.SeekableArray

/**
  * Created by james on 8/30/16.
  */
class DataPageHeaderV2 extends Fauxquetable {
  override def className: String = ???

  override def validate(): Unit = ???

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = ???

  override def doWrite(): Unit = ???
}