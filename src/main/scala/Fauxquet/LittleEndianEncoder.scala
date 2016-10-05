package main.scala.Fauxquet

import java.io.OutputStream

/**
  * Created by james on 10/4/16.
  */
class LittleEndianEncoder {
  val out: OutputStream = null

  def write(b: Int): Unit = this.out.write(b)
  def write(b: Array[Byte], offset: Int, length: Int): Unit = this.out.write(b, offset, length)

  def flush() = this.out.flush()

  def writeBoolean(b: Boolean) = this.out.write(if (b) 1 else 0)
  def writeByte(b: Byte) = this.out.write(b)

  def writeShort(s: Short) = {
    this.out.write(s >>> 0 & 255)
    this.out.write(s >>> 8 & 255)
  }

  def writeInt(i: Int) = {
    this.out.write(i >>> 0 & 255)
    this.out.write(i >>> 8 & 255)
    this.out.write(i >>> 16 & 255)
    this.out.write(i >>> 24 & 255)
  }

  def writeLong(l: Long) = {
    val writeBuffer = new Array[Byte](8)
    writeBuffer(7) = (l >>> 56).asInstanceOf[Int].asInstanceOf[Byte]
    writeBuffer(6) = (l >>> 48).asInstanceOf[Int].asInstanceOf[Byte]
    writeBuffer(5) = (l >>> 40).asInstanceOf[Int].asInstanceOf[Byte]
    writeBuffer(4) = (l >>> 32).asInstanceOf[Int].asInstanceOf[Byte]
    writeBuffer(3) = (l >>> 24).asInstanceOf[Int].asInstanceOf[Byte]
    writeBuffer(2) = (l >>> 16).asInstanceOf[Int].asInstanceOf[Byte]
    writeBuffer(1) = (l >>> 8).asInstanceOf[Int].asInstanceOf[Byte]
    writeBuffer(0) = (l >>> 0).asInstanceOf[Int].asInstanceOf[Byte]

    this.out.write(writeBuffer, 0, 8)
  }

  def writeFloat(f: Float) = {
    this.writeInt(java.lang.Float.floatToIntBits(f))
  }

  def writeDouble(d: Double) = {
    this.writeLong(java.lang.Double.doubleToLongBits(d))
  }
}
