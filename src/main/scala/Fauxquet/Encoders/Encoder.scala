package main.scala.Fauxquet.Encoders

import java.io.OutputStream

/**
  * Created by james on 10/4/16.
  */
trait Encoder {
  var out: OutputStream = _

  def write(b: Int): Unit = this.out.write(b)
  def write(b: Array[Byte]) = this.write(b, 0, b.length)
  def write(b: Array[Byte], offset: Int, length: Int): Unit = this.out.write(b, offset, length)

  def flush() = this.out.flush()

  def writeBoolean(b: Boolean) = this.out.write(if (b) 1 else 0)
  def writeByte(b: Byte) = this.out.write(b)

  def writeShort(s: Short): Unit

  def writeInt(i: Int): Unit

  def writeLong(l: Long): Unit

  def writeFloat(f: Float) = { //change this one?
    this.writeInt(java.lang.Float.floatToIntBits(f))
  }

  def writeDouble(d: Double) = { //maybe change this one?
    this.writeLong(java.lang.Double.doubleToLongBits(d))
  }
}
