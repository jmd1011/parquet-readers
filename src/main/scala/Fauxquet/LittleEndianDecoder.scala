package main.scala.Fauxquet

/**
  * Created by james on 9/17/16.
  */
object LittleEndianDecoder {
  def readBool(arr: SeekableArray[Byte]) = {
    val ch1 = arr next

    if (ch1 < 0) throw new Error("Early EOF")

    ch1 != 0
  }

  def readInt(arr: SeekableArray[Byte]) = {
    val ch4 = arr.next & 255
    val ch3 = arr.next & 255
    val ch2 = arr.next & 255
    val ch1 = arr.next & 255

    (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0)
  }

  def readLong(arr: SeekableArray[Byte]) = {
    val ret = ((arr.next.asInstanceOf[Long] & 255) << 0) + ((arr.next.asInstanceOf[Long] & 255) << 8) + ((arr.next.asInstanceOf[Long] & 255) << 16) + ((arr.next.asInstanceOf[Long] & 255) << 24) + ((arr.next.asInstanceOf[Long] & 255) << 32) + ((arr.next.asInstanceOf[Long] & 255) << 40) + ((arr.next.asInstanceOf[Long] & 255) << 48) + (arr.next.asInstanceOf[Long] << 56)

    ret
  }

  def readDouble(arr: SeekableArray[Byte]) = java.lang.Double.longBitsToDouble(readLong(arr))

  def readFloat(arr: SeekableArray[Byte]) = java.lang.Float.intBitsToFloat(readInt(arr))

  def readString(arr: SeekableArray[Byte]) = {
    val len = readInt(arr)
    readFixedLengthString(arr, len)
  }

  def readFixedLengthString(arr: SeekableArray[Byte], length: Int) = {
    val ret = new Array[Byte](length)

    for (i <- 0 until length) {
      ret(i) = arr next
    }

    new String(ret)
  }

  def readIntPaddedOnBitWidth(arr: SeekableArray[Byte], bitWidth: Int): Int = {
    val bytesWidth = (bitWidth + 7) / 8

    bytesWidth match {
      case 0 => 0
      case 1 => val x = arr.next; if (x < 0) throw new Error("EOF error") else x
      case 2 =>
        val ch1 = arr.next
        val ch2 = arr.next
        if ((ch1 | ch2) < 0)
          throw new Error("EOF error")
        else
          (ch2 << 8) + (ch1 << 0)
      case 3 =>
        val ch1 = arr.next
        val ch2 = arr.next
        val ch3 = arr.next

        if ((ch1 | ch2 | ch3) < 0)
          throw new Error ("EOF error")
        else
          (ch3 << 16) + (ch2 << 8) + (ch1 << 0)
      case 4 => readInt(arr)
      case _ => throw new Error("bytesWidth < 0 || bytesWidth > 4")
    }
  }
}
