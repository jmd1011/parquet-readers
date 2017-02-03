package main.scala.Fauxquet.bytes.BytesInput

import java.io.{ByteArrayOutputStream, OutputStream}

/**
  * Created by james on 1/26/17.
  */
class BaosBytesInput(val arrayOut: ByteArrayOutputStream) extends BytesInput {
  override def writeAllTo(out: OutputStream): Unit = arrayOut.writeTo(out)
  override def size: Long = arrayOut.size()
}
