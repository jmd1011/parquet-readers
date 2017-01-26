package main.scala.Fauxquet.ValuesReaders.bitpacking

/**
  * Created by james on 1/2/17.
  */
abstract class BytePacker(bitWidth: Int) {
  def pack8Values(in: Array[Int], iPos: Int, out: Array[Byte], oPos: Int)
  def pack32Values(in: Array[Int], iPos: Int, out: Array[Byte], oPos: Int)

  def unpack8Values(in: Array[Byte], iPos: Int, out: Array[Int], oPos: Int)
  def unpack32Values(in: Array[Byte], iPos:Int, out: Array[Int], oPos: Int)
}

object BytePacker_BE_1 extends BytePacker(1) {
  override def pack8Values(in: Array[Int], iPos: Int, out: Array[Byte], oPos: Int): Unit =
    out(oPos) = (
                  (
                    (in(0 + iPos) & 1) << 7 | (in(1 + iPos) & 1) << 6 | (in(2 + iPos) & 1) << 5 | (in(3 + iPos) & 1) << 4 |
                    (in(4 + iPos) & 1) << 3 | (in(5 + iPos) & 1) << 2 | (in(6 + iPos) & 1) << 1 | in(7 + iPos) & 1
                  ) & 255
                ).asInstanceOf[Byte]


  override def pack32Values(in: Array[Int], iPos: Int, out: Array[Byte], oPos: Int): Unit = {
    out(0 + oPos) = (((in(0 + iPos) & 1) << 7 | (in(1 + iPos) & 1) << 6 | (in(2 + iPos) & 1) << 5 | (in(3 + iPos) & 1) << 4 | (in(4 + iPos) & 1) << 3 | (in(5 + iPos) & 1) << 2 | (in(6 + iPos) & 1) << 1 | in(7 + iPos) & 1) & 255).asInstanceOf[Byte]
    out(1 + oPos) = (((in(8 + iPos) & 1) << 7 | (in(9 + iPos) & 1) << 6 | (in(10 + iPos) & 1) << 5 | (in(11 + iPos) & 1) << 4 | (in(12 + iPos) & 1) << 3 | (in(13 + iPos) & 1) << 2 | (in(14 + iPos) & 1) << 1 | in(15 + iPos) & 1) & 255).asInstanceOf[Byte]
    out(2 + oPos) = (((in(16 + iPos) & 1) << 7 | (in(17 + iPos) & 1) << 6 | (in(18 + iPos) & 1) << 5 | (in(19 + iPos) & 1) << 4 | (in(20 + iPos) & 1) << 3 | (in(21 + iPos) & 1) << 2 | (in(22 + iPos) & 1) << 1 | in(23 + iPos) & 1) & 255).asInstanceOf[Byte]
    out(3 + oPos) = (((in(24 + iPos) & 1) << 7 | (in(25 + iPos) & 1) << 6 | (in(26 + iPos) & 1) << 5 | (in(27 + iPos) & 1) << 4 | (in(28 + iPos) & 1) << 3 | (in(29 + iPos) & 1) << 2 | (in(30 + iPos) & 1) << 1 | in(31 + iPos) & 1) & 255).asInstanceOf[Byte]
  }

  override def unpack8Values(in: Array[Byte], iPos: Int, out: Array[Int], oPos: Int): Unit = {
    out(0 + oPos) = (in(0 + iPos) & 255) >>> 7 & 1
    out(1 + oPos) = (in(0 + iPos) & 255) >>> 6 & 1
    out(2 + oPos) = (in(0 + iPos) & 255) >>> 5 & 1
    out(3 + oPos) = (in(0 + iPos) & 255) >>> 4 & 1
    out(4 + oPos) = (in(0 + iPos) & 255) >>> 3 & 1
    out(5 + oPos) = (in(0 + iPos) & 255) >>> 2 & 1
    out(6 + oPos) = (in(0 + iPos) & 255) >>> 1 & 1
    out(7 + oPos) = in(0 + iPos) & 255 & 1
  }

  override def unpack32Values(in: Array[Byte], iPos: Int, out: Array[Int], oPos: Int): Unit = {
    out(0 + oPos) = (in(0 + iPos) & 255) >>> 7 & 1
    out(1 + oPos) = (in(0 + iPos) & 255) >>> 6 & 1
    out(2 + oPos) = (in(0 + iPos) & 255) >>> 5 & 1
    out(3 + oPos) = (in(0 + iPos) & 255) >>> 4 & 1
    out(4 + oPos) = (in(0 + iPos) & 255) >>> 3 & 1
    out(5 + oPos) = (in(0 + iPos) & 255) >>> 2 & 1
    out(6 + oPos) = (in(0 + iPos) & 255) >>> 1 & 1
    out(7 + oPos) = in(0 + iPos) & 255 & 1
    out(8 + oPos) = (in(1 + iPos) & 255) >>> 7 & 1
    out(9 + oPos) = (in(1 + iPos) & 255) >>> 6 & 1
    out(10 + oPos) = (in(1 + iPos) & 255) >>> 5 & 1
    out(11 + oPos) = (in(1 + iPos) & 255) >>> 4 & 1
    out(12 + oPos) = (in(1 + iPos) & 255) >>> 3 & 1
    out(13 + oPos) = (in(1 + iPos) & 255) >>> 2 & 1
    out(14 + oPos) = (in(1 + iPos) & 255) >>> 1 & 1
    out(15 + oPos) = in(1 + iPos) & 255 & 1
    out(16 + oPos) = (in(2 + iPos) & 255) >>> 7 & 1
    out(17 + oPos) = (in(2 + iPos) & 255) >>> 6 & 1
    out(18 + oPos) = (in(2 + iPos) & 255) >>> 5 & 1
    out(19 + oPos) = (in(2 + iPos) & 255) >>> 4 & 1
    out(20 + oPos) = (in(2 + iPos) & 255) >>> 3 & 1
    out(21 + oPos) = (in(2 + iPos) & 255) >>> 2 & 1
    out(22 + oPos) = (in(2 + iPos) & 255) >>> 1 & 1
    out(23 + oPos) = in(2 + iPos) & 255 & 1
    out(24 + oPos) = (in(3 + iPos) & 255) >>> 7 & 1
    out(25 + oPos) = (in(3 + iPos) & 255) >>> 6 & 1
    out(26 + oPos) = (in(3 + iPos) & 255) >>> 5 & 1
    out(27 + oPos) = (in(3 + iPos) & 255) >>> 4 & 1
    out(28 + oPos) = (in(3 + iPos) & 255) >>> 3 & 1
    out(29 + oPos) = (in(3 + iPos) & 255) >>> 2 & 1
    out(30 + oPos) = (in(3 + iPos) & 255) >>> 1 & 1
    out(31 + oPos) = in(3 + iPos) & 255 & 1
  }
}

object BytePacker_LE_1 extends BytePacker(1) {
  override def pack8Values(in: Array[Int], iPos: Int, out: Array[Byte], oPos: Int) = {
    out(0 + oPos) = ((in(0 + iPos) & 1 | (in(1 + iPos) & 1) << 1 | (in(2 + iPos) & 1) << 2 | (in(3 + iPos) & 1) << 3 | (in(4 + iPos) & 1) << 4 | (in(5 + iPos) & 1) << 5 | (in(6 + iPos) & 1) << 6 | (in(7 + iPos) & 1) << 7) & 255).asInstanceOf[Byte]
  }

  override def pack32Values(in: Array[Int], iPos: Int, out: Array[Byte], oPos: Int) = {
    out(0 + oPos) = ((in(0 + iPos) & 1 | (in(1 + iPos) & 1) << 1 | (in(2 + iPos) & 1) << 2 | (in(3 + iPos) & 1) << 3 | (in(4 + iPos) & 1) << 4 | (in(5 + iPos) & 1) << 5 | (in(6 + iPos) & 1) << 6 | (in(7 + iPos) & 1) << 7) & 255).asInstanceOf[Byte]
    out(1 + oPos) = ((in(8 + iPos) & 1 | (in(9 + iPos) & 1) << 1 | (in(10 + iPos) & 1) << 2 | (in(11 + iPos) & 1) << 3 | (in(12 + iPos) & 1) << 4 | (in(13 + iPos) & 1) << 5 | (in(14 + iPos) & 1) << 6 | (in(15 + iPos) & 1) << 7) & 255).asInstanceOf[Byte]
    out(2 + oPos) = ((in(16 + iPos) & 1 | (in(17 + iPos) & 1) << 1 | (in(18 + iPos) & 1) << 2 | (in(19 + iPos) & 1) << 3 | (in(20 + iPos) & 1) << 4 | (in(21 + iPos) & 1) << 5 | (in(22 + iPos) & 1) << 6 | (in(23 + iPos) & 1) << 7) & 255).asInstanceOf[Byte]
    out(3 + oPos) = ((in(24 + iPos) & 1 | (in(25 + iPos) & 1) << 1 | (in(26 + iPos) & 1) << 2 | (in(27 + iPos) & 1) << 3 | (in(28 + iPos) & 1) << 4 | (in(29 + iPos) & 1) << 5 | (in(30 + iPos) & 1) << 6 | (in(31 + iPos) & 1) << 7) & 255).asInstanceOf[Byte]
  }

  override def unpack8Values(in: Array[Byte], iPos: Int, out: Array[Int], oPos: Int) = {
    out(0 + oPos) = in(0 + iPos) & 255 & 1
    out(1 + oPos) = (in(0 + iPos) & 255) >>> 1 & 1
    out(2 + oPos) = (in(0 + iPos) & 255) >>> 2 & 1
    out(3 + oPos) = (in(0 + iPos) & 255) >>> 3 & 1
    out(4 + oPos) = (in(0 + iPos) & 255) >>> 4 & 1
    out(5 + oPos) = (in(0 + iPos) & 255) >>> 5 & 1
    out(6 + oPos) = (in(0 + iPos) & 255) >>> 6 & 1
    out(7 + oPos) = (in(0 + iPos) & 255) >>> 7 & 1
  }

  override def unpack32Values(in: Array[Byte], iPos: Int, out: Array[Int], oPos: Int) = {
    out(0 + oPos) = in(0 + iPos) & 255 & 1
    out(1 + oPos) = (in(0 + iPos) & 255) >>> 1 & 1
    out(2 + oPos) = (in(0 + iPos) & 255) >>> 2 & 1
    out(3 + oPos) = (in(0 + iPos) & 255) >>> 3 & 1
    out(4 + oPos) = (in(0 + iPos) & 255) >>> 4 & 1
    out(5 + oPos) = (in(0 + iPos) & 255) >>> 5 & 1
    out(6 + oPos) = (in(0 + iPos) & 255) >>> 6 & 1
    out(7 + oPos) = (in(0 + iPos) & 255) >>> 7 & 1
    out(8 + oPos) = in(1 + iPos) & 255 & 1
    out(9 + oPos) = (in(1 + iPos) & 255) >>> 1 & 1
    out(10 + oPos) = (in(1 + iPos) & 255) >>> 2 & 1
    out(11 + oPos) = (in(1 + iPos) & 255) >>> 3 & 1
    out(12 + oPos) = (in(1 + iPos) & 255) >>> 4 & 1
    out(13 + oPos) = (in(1 + iPos) & 255) >>> 5 & 1
    out(14 + oPos) = (in(1 + iPos) & 255) >>> 6 & 1
    out(15 + oPos) = (in(1 + iPos) & 255) >>> 7 & 1
    out(16 + oPos) = in(2 + iPos) & 255 & 1
    out(17 + oPos) = (in(2 + iPos) & 255) >>> 1 & 1
    out(18 + oPos) = (in(2 + iPos) & 255) >>> 2 & 1
    out(19 + oPos) = (in(2 + iPos) & 255) >>> 3 & 1
    out(20 + oPos) = (in(2 + iPos) & 255) >>> 4 & 1
    out(21 + oPos) = (in(2 + iPos) & 255) >>> 5 & 1
    out(22 + oPos) = (in(2 + iPos) & 255) >>> 6 & 1
    out(23 + oPos) = (in(2 + iPos) & 255) >>> 7 & 1
    out(24 + oPos) = in(3 + iPos) & 255 & 1
    out(25 + oPos) = (in(3 + iPos) & 255) >>> 1 & 1
    out(26 + oPos) = (in(3 + iPos) & 255) >>> 2 & 1
    out(27 + oPos) = (in(3 + iPos) & 255) >>> 3 & 1
    out(28 + oPos) = (in(3 + iPos) & 255) >>> 4 & 1
    out(29 + oPos) = (in(3 + iPos) & 255) >>> 5 & 1
    out(30 + oPos) = (in(3 + iPos) & 255) >>> 6 & 1
    out(31 + oPos) = (in(3 + iPos) & 255) >>> 7 & 1
  }
}