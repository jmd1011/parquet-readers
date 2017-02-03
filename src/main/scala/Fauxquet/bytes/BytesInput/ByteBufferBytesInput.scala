package main.scala.Fauxquet.bytes.BytesInput

import java.io.OutputStream
import java.nio.ByteBuffer
import java.nio.channels.{Channels, WritableByteChannel}

/**
  * Created by james on 1/26/17.
  */
class ByteBufferBytesInput(val byteBuf: ByteBuffer, val offset: Int, val length: Int) extends BytesInput {
  override def writeAllTo(out: OutputStream): Unit = {
    val outputChannel: WritableByteChannel = Channels.newChannel(out)
    byteBuf.position(offset)

    val tempBuf = byteBuf.slice()
    tempBuf.limit(length)

    outputChannel.write(tempBuf)
  }

  override def toByteBuffer: ByteBuffer = {
    byteBuf.position(offset)
    val buf = byteBuf.slice()
    buf.limit(length)
    buf
  }

  override def size: Long = this.length
}
