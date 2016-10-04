package main.scala.Fauxquet.FauxquetObjs

/**
  * Created by james on 8/8/16.
  */
case class TSet(elemType: Byte, size: Int) {
  def this(list: TList) = this(list elemType, list size)
}
