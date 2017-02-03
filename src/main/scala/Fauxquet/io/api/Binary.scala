package main.scala.Fauxquet.io.api

import java.io.{DataOutput, OutputStream}
import java.nio.ByteBuffer
import java.nio.charset.Charset

/**
  * Created by james on 1/30/17.
  *
  * basically a representation for String or Array[Byte]
  */
abstract class Binary extends Comparable[Binary] with Serializable {
  val isBackingBytesReused: Boolean

  def asUtf8String: String
  def length: Int

  def writeTo(out: OutputStream)
  def writeTo(out: DataOutput) //dunno what this is

  def getBytes: Array[Byte]
  def getBytesUnsafe: Array[Byte]

  def slice(start: Int, length: Int): Binary

  def equals(bytes: Array[Byte], offset: Int, length: Int): Boolean
  def equals(bytes: ByteBuffer, offset: Int, length: Int): Boolean
  def equals(other: Binary): Boolean

  override def compareTo(other: Binary): Int
  def compareTo(bytes: Array[Byte], offset: Int, length: Int): Int
  def compareTo(bytes: ByteBuffer, offset: Int, length: Int): Int

  def asByteBuffer: ByteBuffer

  override def equals(o: scala.Any): Boolean = o != null && o.isInstanceOf[Binary] && equals(o.asInstanceOf[Binary])

  def copy(): Binary = {
    this
  }
}

object BinaryManager {
  class ByteBufferBackedBinary(val value: ByteBuffer, ibbr: Boolean, val o: Int = -1, val l: Int = -1) extends Binary {
    val offset = if (o == -1) value.arrayOffset() else o
    val len = if (l == -1) value.limit() else l

    override val isBackingBytesReused: Boolean = ibbr
    var cachedBytes: Array[Byte] = _

    override def asUtf8String: String = {
      if (value.hasArray) {
        new String(value.array(), value.arrayOffset() + offset, len, "UTF-8")
      } else {
        val limit = value.limit()
        value.limit(offset + len)

        val position = value.position()
        value.position(offset)

        val ret = Charset.forName("UTF-8").decode(value).toString
        value.limit(limit)
        value.position(position)

        ret
      }
    }

    override def length: Int = len

    override def writeTo(out: OutputStream): Unit = {
      if (value.hasArray) {
        out.write(value.array(), value.arrayOffset() + offset, len)
      } else {
        out.write(getBytesUnsafe, 0, len)
      }
    }

    override def writeTo(out: DataOutput): Unit = ???

    override def getBytes: Array[Byte] = {
      val bytes = new Array[Byte](len)

      val limit = value.limit()
      value.limit(offset + len)

      val position = value.position()
      value.position(offset)
      value.get(bytes)
      value.limit(limit)
      value.position(position)

      if (!isBackingBytesReused) {
        cachedBytes = bytes
      }

      bytes
    }

    override def getBytesUnsafe: Array[Byte] = if (cachedBytes != null) cachedBytes else getBytes

    override def slice(start: Int, length: Int): Binary = BinaryManager.fromConstantByteArray(getBytesUnsafe, start, length)

    override def equals(bytes: Array[Byte], offset: Int, length: Int): Boolean =
      if (value.hasArray)
        BinaryManager.equals(value.array(), value.arrayOffset() + this.offset, len, bytes, offset, length)
      else
        BinaryManager.equals(bytes, offset, length, value, this.offset, len)


    override def equals(bytes: ByteBuffer, offset: Int, length: Int): Boolean = ???

    override def equals(other: Binary): Boolean =
      if (value.hasArray)
        other.equals(value.array(), value.arrayOffset() + offset, len)
      else
        other.equals(value, offset, len)

    override def compareTo(other: Binary): Int = ???

    override def compareTo(bytes: Array[Byte], offset: Int, length: Int): Int = ???

    override def compareTo(bytes: ByteBuffer, offset: Int, length: Int): Int = ???

    override def asByteBuffer: ByteBuffer = ???
  }

  class ByteArrayBackedBinary(val value: Array[Byte], ibbr: Boolean) extends Binary {
    override val isBackingBytesReused: Boolean = ibbr

    override def asUtf8String: String = ???

    override def length: Int = ???

    override def writeTo(out: OutputStream): Unit = ???

    override def writeTo(out: DataOutput): Unit = ???

    override def getBytes: Array[Byte] = ???

    override def getBytesUnsafe: Array[Byte] = ???

    override def slice(start: Int, length: Int): Binary = ???

    override def equals(bytes: Array[Byte], offset: Int, length: Int): Boolean = ???

    override def equals(bytes: ByteBuffer, offset: Int, length: Int): Boolean = ???

    override def equals(other: Binary): Boolean = ???

    override def compareTo(other: Binary): Int = ???

    override def compareTo(bytes: Array[Byte], offset: Int, length: Int): Int = ???

    override def compareTo(bytes: ByteBuffer, offset: Int, length: Int): Int = ???

    override def asByteBuffer: ByteBuffer = ???
  }

  class ByteArraySliceBackedBinary(val value: Array[Byte], var offset: Int, val length: Int, ibbr: Boolean) extends Binary {
    override val isBackingBytesReused: Boolean = ibbr

    override def asUtf8String: String = ???

    override def writeTo(out: OutputStream): Unit = ???

    override def writeTo(out: DataOutput): Unit = ???

    override def getBytes: Array[Byte] = ???

    override def getBytesUnsafe: Array[Byte] = ???

    override def slice(start: Int, length: Int): Binary = ???

    override def equals(bytes: Array[Byte], offset: Int, length: Int): Boolean = ???

    override def equals(bytes: ByteBuffer, offset: Int, length: Int): Boolean = ???

    override def equals(other: Binary): Boolean = ???

    override def compareTo(other: Binary): Int = ???

    override def compareTo(bytes: Array[Byte], offset: Int, length: Int): Int = ???

    override def compareTo(bytes: ByteBuffer, offset: Int, length: Int): Int = ???

    override def asByteBuffer: ByteBuffer = ???
  }

  class FromStringBinary(val vl: String) extends ByteBufferBackedBinary(ByteBuffer.wrap(vl.getBytes("UTF-8")), false) {

  }

  def fromString(str: String): Binary = {
    new FromStringBinary(str) //TODO: I think this might be the line breaking everything -- only difference right now (thought it's what's in Parquet)
  }

  def equals(array1: Array[Byte], offset1: Int, length1: Int, array2: Array[Byte], offset2: Int, length2: Int): Boolean = {
    if (array1 == null && array2 == null) return true
    else if (array1 == null || array2 == null) return false
    else if (length1 != length2) return false
    else if ((array1 sameElements array2) && offset1 == offset2) return true
    else {
      for (i <- 0 until length1) {
        if (array1(i + offset1) != array2(i + offset2)) return false
      }
    }

    true
  }

  def equals(array: Array[Byte], offset: Int, length: Int, buf: ByteBuffer, offset2: Int, length2: Int): Boolean = {
    if (array == null && buf == null) return true
    if (array == null || buf == null) return false
    if (length != length2) return false

    for (i <- 0 until length) {
      if (array(i + offset) != buf.get(i + offset2)) {
        return false
      }
    }

    true
  }

  def fromConstantByteArray(value: Array[Byte]): Binary = new ByteArrayBackedBinary(value, false)

  def fromConstantByteArray(value: Array[Byte], offset: Int, length: Int): Binary = new ByteArraySliceBackedBinary(value, offset, length, false)
}