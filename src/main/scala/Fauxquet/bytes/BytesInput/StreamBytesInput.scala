package main.scala.Fauxquet.bytes.BytesInput

import java.io.{InputStream, OutputStream}

/**
  * Created by james on 1/26/17.
  */
object StreamBytesInput extends BytesInput {
  var in: InputStream = _
  var byteCount = 0

  def apply(in: InputStream, byteCount: Int) = {
    this.in = in
    this.byteCount = byteCount
  }

  override def writeAllTo(out: OutputStream): Unit = out.write(this.toByteArray)

  override def size = byteCount
}
