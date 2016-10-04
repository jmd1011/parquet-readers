package main.scala.Fauxquet.FauxquetObjs

/**
  * Created by james on 8/9/16.
  */
object EncodingManager {
  def getEncodingById(id: Int): Encoding = id match {
    case 0 => Encoding(0, "PLAIN")
      //umm wat? where's 1?
    case 2 => Encoding(0, "PLAIN_DICTIONARY")
    case 3 => Encoding(0, "RLE")
    case 4 => Encoding(0, "BIT_PACKED")
    case 5 => Encoding(0, "DELTA_BINARY_PACKED")
    case 6 => Encoding(0, "DELTA_LENGTH_BYTE_ARRAY")
    case 7 => Encoding(0, "DELTA_BYTE_ARRAY")
    case 8 => Encoding(0, "RLE_DICTIONARY")
  }
}

case class Encoding(id: Int, value: String)