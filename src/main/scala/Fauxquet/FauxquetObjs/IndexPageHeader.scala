package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet.{Fauxquetable, SeekableArray}

/**
  * Created by james on 8/30/16.
  */
class IndexPageHeader extends Fauxquetable {
  override def className: String = "IndexPageHeader"

  override def write(): Unit = ???

  override def validate(): Unit = ???

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = ???
}
