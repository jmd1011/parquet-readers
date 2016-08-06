package main.scala.Fauxquet

/**
  * Created by james on 8/5/16.
  */

class FauxquetDecoder(var id: Int = 0) {
  var boolValue: Boolean = null //wtf is this?

  def readFieldBegin(arr: SeekableArray[Byte]): (TField) = {
    val t = arr next

    if (t == 0) TSTOP

    val modifier = ((t & 240) >> 4) asInstanceOf[Short]
    var fid: Short = null

    if (modifier == 0) {
      fid = readI16(arr)
    }
    else
      fid = (this.id + modifier) asInstanceOf[Short]

    val field = new TField("", getType((t & 15).asInstanceOf[Byte]), fid)

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

  def readVarint32(arr: SeekableArray[Byte]): Int = {
    var res = 0
    var shift = 0

    if (arr.length - arr.pos >= 5) {
      var offset = 0

      while (true) {
        val b1: Byte = arr.next
        res |= (b1 & 127) << shift

        if ((b1 & 128) != 128) {
          return res
        }

        shift += 7
      }

      res //just to shut IntelliJ up
    } else {
      throw new Error("Not implemented")
      //while (true) ???
    }
  }

  def zigzagToInt(n: Int): Int = n >>> 1 ^ -(n & 1)
}