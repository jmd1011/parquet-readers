package main.scala.Fauxquet.bytes.BytesInput

import java.io.{ByteArrayOutputStream, OutputStream}

/**
  * Created by james on 1/26/17.
  */
object BaosBytesInput extends BytesInput {
  var arrayOut: ByteArrayOutputStream = _

  def apply(arrayOut: ByteArrayOutputStream) = this.arrayOut = arrayOut

  override def writeAllTo(out: OutputStream): Unit = arrayOut.writeTo(out)
  override def size: Long = arrayOut.size()
}
