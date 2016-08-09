package main.scala.Fauxquet

/**
  * Created by james on 8/5/16.
  */
class SeekableArray[T](val array: Array[T], var pos: Int = 0) {
  def apply(i: Int) = {
    pos = i + 1
    array(i)
  }

  def length = array length

  def next = apply(pos)

  def getItemWithoutMovingPos(i: Int) = array(i)

  def readAll(array: Array[T], length: Int) = {
    for (i <- 0 until length) {
      array(i) = this.next
    }
  }

  def getBytesRemaining: Int = array.length - pos
}
