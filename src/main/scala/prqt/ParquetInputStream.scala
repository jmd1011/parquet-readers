package main.scala.prqt
import java.nio.ByteBuffer

import org.apache.hadoop.fs.FSDataInputStream

/**
  * Created by jdecker on 7/1/16.
  */
class ParquetInputStream(stream: FSDataInputStream) extends SeekableInputStream {
  val COPY_BUFFER_SIZE = 8192
  val temp = new Array[Byte](COPY_BUFFER_SIZE)

  override def position: Long = stream getPos

  override def seek(newPos: Long): Unit = stream seek newPos

  override def read(): Int = stream read

  override def read(b: Array[Byte], off: Int, len: Int): Int = stream read(b, off, len)

  override def read(buf: ByteBuffer): Int = if (buf hasArray) readHeapBuffer(buf) else readDirectBuffer(buf)

  override def readFully(bytes: Array[Byte]): Unit = stream readFully(bytes, 0, bytes.length)

  override def readFully(bytes: Array[Byte], start: Int, len: Int): Unit = stream readFully(bytes, start, len)

  override def readFully(buf: ByteBuffer): Unit = if (buf hasArray) readFullyHeapBuffer(buf) else readFullyDirectBuffer(buf)

  override def close(): Unit = stream.close()

  def readHeapBuffer(buf: ByteBuffer): Int = {
    val bytesRead = stream.read(buf array(), buf.arrayOffset() + buf.position(), buf remaining())

    if (bytesRead > 0) buf.position(buf.position() + bytesRead)

    bytesRead
  }

  def readFullyHeapBuffer(buf: ByteBuffer): Unit = {
    stream.readFully(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining())
    buf position buf.limit
  }

  def nextReadLength(buf: ByteBuffer): Int = math min(buf remaining(), temp length)

  def readDirectBuffer(buf: ByteBuffer): Int = {


    var totalBytesRead = 0
    var bytesRead = stream read(temp, 0, nextReadLength(buf))

    while (bytesRead == temp.length) {
      buf put temp
      totalBytesRead += bytesRead
      bytesRead = stream read(temp, 0, nextReadLength(buf))
    }


    if (bytesRead < 0) {
      if (totalBytesRead == 0) totalBytesRead = -1
    } else {
      buf put(temp, 0, bytesRead)
      totalBytesRead += bytesRead
    }

    totalBytesRead
  }

  def readFullyDirectBuffer(buf: ByteBuffer): Unit = {
    var nrl = nextReadLength(buf)
    var bytesRead = stream read(temp, 0, nrl)

    while (nextReadLength(buf) > 0 && bytesRead >= 0) {
      buf put(temp, 0, bytesRead)
      nrl = nextReadLength(buf)
      bytesRead = stream read(temp, 0, nrl)
    }

    if (bytesRead < 0 && buf.remaining() > 0) {
      throw new Exception(s"Reached the end of stream. Still have: ${buf.remaining()} bytes left.")
    }
  }
}