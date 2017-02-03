package main.scala.Fauxquet.bytes.BytesInput
import java.io.OutputStream
import java.nio.ByteBuffer

/**
  * Created by james on 1/26/17.
  */
class ByteArrayBytesInput(val in: Array[Byte], val offset: Int, val length: Int) extends BytesInput {
  override def writeAllTo(out: OutputStream): Unit = {
    out.write(in, offset, length)
  }

  override def toByteBuffer: ByteBuffer = {
    ByteBuffer.wrap(in, offset, length)
  }

  override def size: Long = length
}
