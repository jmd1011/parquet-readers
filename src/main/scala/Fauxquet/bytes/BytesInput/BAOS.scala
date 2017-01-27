package main.scala.Fauxquet.bytes.BytesInput

import java.io.ByteArrayOutputStream

/**
  * Created by james on 1/26/17.
  */
object BAOS extends ByteArrayOutputStream {
  def apply(size: Int): Unit = this.buf = new Array[Byte](size) //this is icky -- talk to Tiark?
  def getBuf = buf
}