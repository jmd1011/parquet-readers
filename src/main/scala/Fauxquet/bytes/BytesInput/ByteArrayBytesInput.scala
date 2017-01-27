package main.scala.Fauxquet.bytes.BytesInput
import java.io.OutputStream
import java.nio.ByteBuffer

/**
  * Created by james on 1/26/17.
  */
object ByteArrayBytesInput extends BytesInput {
  var in: Array[Byte] = _
  var offset: Int = 0
  var length: Int = 0

  def apply(in: Array[Byte], offset: Int, length: Int) = {
    this.in = in
    this.offset = offset
    this.length = length
  }

  override def writeAllTo(out: OutputStream): Unit = {
    out.write(in, offset, length)
  }

  override def toByteBuffer: ByteBuffer = {
    ByteBuffer.wrap(in, offset, length)
  }

  override def size: Long = length
}
