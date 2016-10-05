package main.scala.Fauxquet

import Encoders.Encoder
import main.scala.Fauxquet.FauxquetObjs.{TField, TList, TMap, TSet}

/**
  * Created by james on 8/5/16.
  */
object FauxquetEncoder {
  var id: Int = 0
  //var boolValue: Boolean = _
  var boolField: TField = _

  var encoder: Encoder = _

  def writeStructBegin() = this.id = 0
  def writeStructEnd(id: Int) = this.id = id

  def writeFieldBegin(field: TField) = {
    def writeFieldBegin(field: TField, typeOverride: Byte) = {
      val typeToWrite = if (typeOverride == -1) this.getCompactType(field.Type) else typeOverride

      if (field.id > this.id && field.id - this.id <= 15) {
        this.writeByteDirect(field.id - this.id << 4 | typeToWrite)
      } else {
        this.writeByteDirect(typeToWrite)
        this.writeI16(field.id)
      }

      id = field.id
    }

    if (field.Type == 2) {
      this.boolField = field
    } else {
      writeFieldBegin(field, -1)
    }
  }
  def writeFieldEnd() = {}

  def writeFieldStop() = this.writeByteDirect(0.asInstanceOf[Byte])

  def writeByteDirect(byte: Byte) = {
    this.encoder.write(Array[Byte](byte), 0, 1)
  }

  def writeByteDirect(int: Int) = {
    this.writeByteDirect(int.asInstanceOf[Byte])
  }

  def writeVarint32(int: Int) = {
    var i = 0
    val buf = new Array[Byte](5)
    var n = int

    while ((n & -128) != 0) {
      buf(i) = (n & 127 | 128).asInstanceOf[Byte]
      i += 1
      n >>>= 7
    }

    buf(i) = n.asInstanceOf[Byte]
    i += 1

    this.encoder.write(buf, 0, i)
  }

  def writeVarint64(l: Long) = {
    var i = 0
    var buf = new Array[Byte](10)
    var n = l

    while ((n & -128L) != 0L) {
      buf(i) = (n & 127L | 128L).asInstanceOf[Int].asInstanceOf[Byte]
      i += 1
      n >>>= 7
    }

    buf(i) = n.asInstanceOf[Int].asInstanceOf[Byte]
    i += 1

    this.encoder.write(buf, 0, i)
  }

  def intToZigZag(n: Int): Int = n << 1 ^ n >> 31
  def longToZigZag(n: Long): Long = n << 1 ^ n >> 63

  def fixedLongToBytes(n: Long, buf: Array[Byte], offset: Int) = {
    buf(offset + 0) = (n & 255L).asInstanceOf[Int].asInstanceOf[Byte]
    buf(offset + 1) = (n >> 8 & 255L).asInstanceOf[Int].asInstanceOf[Byte]
    buf(offset + 2) = (n >> 16 & 255L).asInstanceOf[Int].asInstanceOf[Byte]
    buf(offset + 3) = (n >> 24 & 255L).asInstanceOf[Int].asInstanceOf[Byte]
    buf(offset + 4) = (n >> 32 & 255L).asInstanceOf[Int].asInstanceOf[Byte]
    buf(offset + 5) = (n >> 40 & 255L).asInstanceOf[Int].asInstanceOf[Byte]
    buf(offset + 6) = (n >> 48 & 255L).asInstanceOf[Int].asInstanceOf[Byte]
    buf(offset + 7) = (n >> 56 & 255L).asInstanceOf[Int].asInstanceOf[Byte]
  }

  def writeByte(byte: Byte) = this.writeByteDirect(byte)

  def writeI16(n: Short) = this.writeVarint32(this.intToZigZag(n))
  def writeI32(n: Int) = this.writeVarint32(this.intToZigZag(n))
  def writeI64(n: Long) = this.writeVarint64(this.longToZigZag(n))

  def writeDouble(n: Double) = {
    val data = Array[Byte]()
    this.fixedLongToBytes(java.lang.Double.doubleToLongBits(n), data, 0)
    this.encoder.write(data)
  }

  def writeString(str: String) = {
    val arr = str.getBytes("UTF-8")
    this.writeBinary(arr, 0, arr.length)
  }

  def writeBinary(buf: Array[Byte], offset: Int, length: Int)  = {
    this.writeVarint32(length)
    this.encoder.write(buf, offset, length)
  }

  def writeMapBegin(map: TMap) = {
    if (map.size == 0) this.writeByteDirect(0)
    else {
      this.writeVarint32(map size)
      this.writeByteDirect(this.getCompactType(map.keyType) << 4 | this.getCompactType(map.valueType))
    }
  }

  def writeCollectionBegin(elemType: Byte, size: Int) = {
    if (size <= 14) {
      this.writeByteDirect(size << 4 | this.getCompactType(elemType))
    } else {
      this.writeByteDirect(240 | this.getCompactType(elemType))
      this.writeVarint32(size)
    }
  }

  def writeListBegin(list: TList) = {
    this.writeCollectionBegin(list elemType, list size)
  }

  def writeSetBegin(set: TSet) = {
    this.writeCollectionBegin(set elemType, set size)
  }

  def getCompactType(Type: Byte): Byte = Type match {
    case 0 => 0
    case 2 => 1
    case 3 => 3
    case 6 => 4
    case 8 => 5
    case 10 => 6
    case 4 => 7
    case 11 => 8
    case 15 => 9
    case 14 => 10
    case 13 => 11
    case 12 => 12
    case _ => throw new Error("Ummm awkward")
  }

}
