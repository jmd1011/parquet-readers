package main.scala.Fauxquet.Encoders

import java.io.OutputStream

/**
  * Created by james on 1/27/17.
  */
class PlainEncoder(out: OutputStream) extends Encoder(out) {
  override def writeShort(s: Short): Unit = out.write(s)

  override def writeInt(i: Int): Unit = out.write(i)

  override def writeLong(l: Long): Unit = out.write(l.asInstanceOf[Int]) //TODO: Fix this eventually, but shouldn't get called due to ZigZag calculations
}
