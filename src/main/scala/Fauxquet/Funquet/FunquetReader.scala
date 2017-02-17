package main.scala.Fauxquet.Funquet

import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

/**
  * Created by james on 2/11/17.
  */
class FunquetReader(path: String) {
  // def readListBegin(i: Int): (Int, TList) = {
  // 	val sizeAndType = array(i)

  // 	val size = sizeAndType >> 4 & 15

  // 	if (size != 15) (i + 1, new TList(getType(sizeAndType), size))
  // 	else {
  // 		val (i1, size1) = readVarint32(i + 1)
  // 		(i1, new TList(getType(sizeAndType), size1))
  // 	}
  // }

 //  def readFieldBegin(i: Int, id: Int = 0): (Int, TField) = {
 //  	val t = array(i)

 //  	if (t == 0) TSTOP
 //  	else {
 //  		val modifier = ((t & 240) >> 4).asInstanceOf[Short]

 //  		val (i1, fid) = if (modifier == 0) readI16(i + 1) else (i + 1, (id + modifier).asInstanceOf[Short])
  		
 //  		val field = new TField(getType((t & 15).asInstanceOf[Byte]), fid)

 //  		if (isBoolean(t)) this.boolValue = (t & 15).asInstanceOf[Byte] == 1
 //  		this.id = fid

 //  		(i1, field)
 //  	}
 //  }

 //  def isBoolean(byte: Byte): Boolean = {
 //  	val l = byte & 15
 //  	l == 1 || l == 2
 //  }

 //  def getType(t: Byte): Byte = (t & 15).asInstanceOf[Byte] match {
 //  	case 0 | 3 | 12 => (t & 15).asInstanceOf[Byte]
 //  	case 1 | 2 => 2
 //  	case 4 => 6
 //  	case 5 => 8
 //  	case 6 => 10
 //  	case 7 => 4
 //  	case 8 => 11
 //  	case 9 => 15
 //  	case 10 => 14
 //  	case 11 => 13
 //  	case _ => throw new Error(s"Unable to match type ${(t & 15).asInstanceOf[Byte]}")
 //  }

 //  def readI16(i: Int): (Int, Short) = {
 //  	val (i1, r) = readVarint32(i)
 //  	val ret = zigzagToInt(r)
 //  	ret.asInstanceOf[Short]
 //  }

 //  def readI32(i: Int): (Int, Int) = {
 //  	val (i1, r) = readVarint32(i)
 //  	val ret = zigzagToInt(r)
 //  	(i1, ret)
 //  }

 //  def readI64(i: Int): (Int, Long) = {
 //  	val (i1, r) = readVarint64(i)
 //  	val ret = zigzagToLong(r)
 //  	(i1, ret)
 //  }

  // def readVarint32(i: Int): (Int, Int) = {
  // 	def readVarint32(i: Int, shift: Int, res: Int) {
  // 		val b1 = array(i)
  // 		val res1 = res | ((b1 & 127) << shift)

  // 		if ((b1 & 128) != 128) (i + 1, res1)
  // 		else readVarint32(i + 1, shift + 7, res1)
  // 	}

  // 	readVarint32(i, 0, 0)
  // }

  // def readVarint64(i: Int): (Int, Int) = {
  // 	def readVarint64(i: Int, shift: Int, res: Long) {
  // 		val b1 = array(i)
  // 		val res1 = res | ((b1 & 127).asInstanceOf[Long] << shift)

  // 		if ((byte & 128) != 128) (i + 1, res1)
  // 		else readVarint64(i + 1, shift + 7, res1)
  // 	}

  // 	readVarint64(i, 0, 0)
  // }

  // def zigzagToInt(n: Int):	 Int  = n >>> 1 ^ -(n & 1)
  // def zigzagToLong(n: Long): Long = n >>> 1 ^ -(n & 1L)

 //  var boolValue: Boolean = _
 //  var id: Int = 0

 //  val array = Files.readAllBytes(Paths.get(path))
 //  val MAGIC = "PAR1".getBytes(Charset.forName("ASCII"))

 //  val fileMetadataIndex(array: Array[Byte]): Int = {
 //  	val l = array.length

 //  	val footerLengthIndex = l - 4 - MAGIC.length
 //  	val footerLength = {
 //  		val x1 = array(footerLengthIndex) & 255
 //  		val x2 = array(footerLengthIndex + 1) & 255
 //  		val x3 = array(footerLengthIndex + 2) & 255
 //  		val x4 = array(footerLengthIndex + 3) & 255

 //  		if ((x1 | x2 | x3 | x4) < 0) throw new Error("Hit EOF early")

 //  		(x4 << 24) + (x3 << 16) + (x2 << 8) + (x1 << 0)
 //  	}

 //  	val magic = new Array[Byte](MAGIC.length)

	// for (i <- 0 until MAGIC.length) {
	// 	magic(i) = array(footerLengthIndex + 4 + i)
	// }

	// if (magic.sameElements(MAGIC)) throw new Error("Not a Parquet File")

	// footerLengthIndex - footerLength
 //  }

 //  val (version, numRows, createdBy, schema) = {  	
 //  	def doMatch(field: TField) = {
 //  		case TField(8, 1) => version
 //  	}
 //  }

  
  

  // case class TList(elemType: Byte, size: Int)
  // case class TField(Type: Byte, id: Short)
  // object TSTOP extends TField(0, 0)
}