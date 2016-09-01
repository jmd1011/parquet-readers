package main.scala.Fauxquet

import java.nio.ByteBuffer

/**
  * Created by james on 8/5/16.
  */

object FauxquetDecoder {
  var id: Int = 0
  var boolValue: java.lang.Boolean = _ //wtf is this?
  //var ids: List[Int] = Nil

  def readStructBegin(): Unit = id = 0
  def readStructEnd(id: Int) = this.id = id

  //TODO
  def readSchemaItem(arr: SeekableArray[Byte]): String = {
    ""
  }

  def skip(arr: SeekableArray[Byte], Type: Byte): Unit = {
    def skip0(arr: SeekableArray[Byte], Type: Byte, maxDepth: Int): Unit = {
      if (maxDepth <= 0) throw new Error("Maximum skip depth exceeded")

      Type match {
        case 2 => readBool(arr)
        case 3 => readByte(arr)
        case 4 => readDouble(arr)
        case 6 => readI16(arr)
        case 8 => readI32(arr)
        case 10 => readI64(arr)
        case 11 => readBinary(arr)
        case 12 =>
          id = 0

          var keepGoing = true

          while (keepGoing) {
            val field = readFieldBegin(arr)

            if (field.Type == 0) {
              id = field.id
              keepGoing = false
            }

            if (keepGoing)
              skip0(arr, field Type, maxDepth - 1)
          }
        case 13 =>
          val map = readMapBegin(arr)

          for (i <- 0 until map.size) {
            skip0(arr, map keyType, maxDepth - 1)
            skip0(arr, map valueType, maxDepth - 1)
          }

        case 14 =>
          val set = readSetBegin(arr)

          for (i <- 0 until set.size) {
            skip0(arr, set elemType, maxDepth - 1)
          }

        case 15 =>
          val list = readListBegin(arr)

          for (i <- 0 until list.size) {
            skip0(arr, list elemType, maxDepth - 1)
          }
      }
    }

    skip0(arr, Type, 2147483647)
  }

  def readMapBegin(arr: SeekableArray[Byte]): TMap = {
    val size = readVarint32(arr)
    val keyAndValueType: Byte = if (size == 0) 0 else readByte(arr)
    TMap(getType((keyAndValueType >> 4).asInstanceOf[Byte]), getType((keyAndValueType & 15).asInstanceOf[Byte]), size)
  }

  def readSetBegin(arr: SeekableArray[Byte]): TSet = {
    new TSet(readListBegin(arr))
  }

  def readListBegin(arr: SeekableArray[Byte]): TList = {
    val sizeAndType = readByte(arr)
    var size = sizeAndType >> 4 & 15

    if (size == 15) size = readVarint32(arr)

    val Type = getType(sizeAndType)
    TList(Type, size)
  }

  def readFieldBegin(arr: SeekableArray[Byte], id: Int = 0): TField = {
    val t = arr next

    if (t == 0) return TSTOP

    val modifier = ((t & 240) >> 4).asInstanceOf[Short]
    var fid: Short = 0
    this.id = id

    if (modifier == 0) {
      fid = readI16(arr)
    }
    else
      fid = (this.id + modifier).asInstanceOf[Short]

    val field = TField("", getType((t & 15).asInstanceOf[Byte]), fid)

    if (isBoolean(t)) this.boolValue = (t & 15).asInstanceOf[Byte] == 1

    this.id = fid
    field
  }

  def isBoolean(byte: Byte): Boolean = {
    val l = byte & 15
    l == 1 || l == 2
  }

  def getType(t: Byte): Byte = (t & 15).asInstanceOf[Byte] match {
    case 0 | 3 | 12 => (t & 15).asInstanceOf[Byte]
    case 1 | 2 => 2
    case 4 => 6
    case 5 => 8
    case 6 => 10
    case 7 => 4
    case 8 => 11
    case 9 => 15
    case 10 => 14
    case 11 => 13
    case _ => throw new Error(s"Unable to match type ${(t & 15).asInstanceOf[Byte]}")
  }

  def readI16(arr: SeekableArray[Byte]): Short = {
    val r = readVarint32(arr)
    val ret = zigzagToInt(r)
    ret.asInstanceOf[Short]
  }

  def readI32(arr: SeekableArray[Byte]): Int = {
    val r = readVarint32(arr)
    val ret = zigzagToInt(r)
    ret
  }

  def readLong(b: java.io.ByteArrayInputStream, len: Int): Long = {
    var count = 0
    var n = 0

    val ab = new Array[Byte](len)

    while (n < len) {

      count = b.read(ab, 0, len)

      if (count < 0) throw new Error("Hit end of file early")

      n += count
    }

    val ret = {
      var sum = 0L

      for (i <- 0 until 8) {
        sum += (ab(i).asInstanceOf[Long] & 255) << (8 * i)
      }

      sum
    }

    ret
  }

  def readLong(arr: SeekableArray[Byte]): Long = {
    val ret = {
      var sum = 0L

      for (i <- 0 until 8) {
        sum += (arr.next.asInstanceOf[Long] & 255) << (8 * i)
      }

      sum
    }

    //val ret = ((long)this.readBuffer[arr.pos + 7] << 56) + ((long)(this.readBuffer[6] & 255) << 48) + ((long)(this.readBuffer[5] & 255) << 40) + ((long)(this.readBuffer[4] & 255) << 32) + ((long)(this.readBuffer[3] & 255) << 24) + (long)((this.readBuffer[2] & 255) << 16) + (long)((this.readBuffer[1] & 255) << 8) + (long)((this.readBuffer[0] & 255) << 0)

    ret
  }

  def readI64(arr: SeekableArray[Byte]): Long = {
    val r = readVarint64(arr)
    val ret = zigzagToLong(r)
    ret
  }

  def readVarint32(arr: SeekableArray[Byte]): Int = {
    var res = 0
    var shift = 0

    while (true) {
      val b1: Byte = arr.next
      res |= (b1 & 127) << shift

      if ((b1 & 128) != 128) {
        return res
      }

      shift += 7
    }

    res
  }

  def readVarint64(arr: SeekableArray[Byte]): Long = {
    var shift = 0
    var res = 0L

    while (true) {
      val byte = arr.next
      res |= (byte & 127).asInstanceOf[Long] << shift

      if ((byte & 128) != 128) {
        return res
      }

      shift += 7
    }

    res
  }

  def zigzagToInt(n: Int): Int = n >>> 1 ^ -(n & 1)
  def zigzagToLong(n: Long): Long = n >>> 1 ^ -(n & 1L)

  def readBinary(arr: SeekableArray[Byte]): ByteBuffer = {
    val length = readVarint32(arr)

    if (length == 0) ByteBuffer.wrap(new Array[Byte](0))
    else {
      val buf = new Array[Byte](length)
      arr.readAll(buf, length)
      ByteBuffer.wrap(buf)
    }
  }

  def readBinary(arr: SeekableArray[Byte], length: Int) = {
    if (length == 0) new Array[Byte](0)
    else {
      val buf = new Array[Byte](length)
      arr.readAll(buf, length)
      buf
    }
  }

  def readBool(arr: SeekableArray[Byte]): Boolean = {
    if (boolValue != null) {
      val res = boolValue.booleanValue()
      boolValue = null
      return res
    }

    readByte(arr) == 1
  }

  def readByte(arr: SeekableArray[Byte]): Byte = {
    arr next
  }

  def readString(arr: SeekableArray[Byte]): String = {
    val length = readVarint32(arr)

    if (length == 0) ""
    else {
//      if (arr.getBytesRemaining >= length) {
//        val e: String = new String(arr.array, arr.pos, length, "UTF-8")
//        arr.pos += length
//
//        e
//      } else {
        new String(readBinary(arr, length), "UTF-8")
      //}
    }
  }

  def readDouble(arr: SeekableArray[Byte]): Double = {
    val longBits = new Array[Byte](8)
    arr.readAll(longBits, 8)
    java.lang.Double.longBitsToDouble(bytesToLong(longBits))
  }

  def bytesToLong(bytes: Array[Byte]): Long = {
    (bytes(7).asInstanceOf[Long] & 255L) << 56 | (bytes(6).asInstanceOf[Long] & 255L) << 48 | (bytes(5).asInstanceOf[Long] & 255L) << 40 | (bytes(4).asInstanceOf[Long] & 255L) << 32 | (bytes(3).asInstanceOf[Long] & 255L) << 24 | (bytes(2).asInstanceOf[Long] & 255L) << 16 | (bytes(1).asInstanceOf[Long] & 255L) << 8 | bytes(0).asInstanceOf[Long] & 255L
  }
}