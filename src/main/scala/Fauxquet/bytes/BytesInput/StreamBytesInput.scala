package main.scala.Fauxquet.bytes.BytesInput

import java.io.{DataInputStream, InputStream, OutputStream}

/**
  * Created by james on 1/26/17.
  */
class StreamBytesInput(val in: InputStream, val byteCount: Int) extends BytesInput {
  override def writeAllTo(out: OutputStream): Unit = out.write(this.toByteArray)

  override def toByteArray: Array[Byte] = {
    val buf = new Array[Byte](byteCount)
    new DataInputStream(in).readFully(buf)
    buf
  }

  override def size = byteCount
}
