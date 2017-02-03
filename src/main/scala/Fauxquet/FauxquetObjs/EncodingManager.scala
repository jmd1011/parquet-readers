package main.scala.Fauxquet.FauxquetObjs

/**
  * Created by james on 8/9/16.
  *
  * TODO: Email Parquet about this
  */
object EncodingManager {
  def getEncodingById(id: Int): Encoding = id match {
    case 0 => PLAIN
    case 1 => null//RLE
    case 2 => PLAIN_DICTIONARY//BIT_PACKED
    case 3 => RLE//PLAIN_DICTIONARY
    case 4 => BIT_PACKED//DELTA_BINARY_PACKED
    case 5 => null//DELTA_LENGTH_BYTE_ARRAY
    case 6 => null//DELTA_BYTE_ARRAY
    case 7 => null//RLE_DICTIONARY
  }
}

//case class Encoding(id: Int, value: String)

trait Encoding {
  val id: Int
  val value: String
}

object PLAIN extends Encoding {
  override val id: Int = 0
  override val value: String = "PLAIN"
}

object RLE extends Encoding {
  override val id: Int = 3
  override val value: String = "RLE"
}

object BIT_PACKED extends Encoding {
  override val id: Int = 4
  override val value: String = "BIT_PACKED"
}

object PLAIN_DICTIONARY extends Encoding {
  override val id: Int = 2
  override val value: String = "PLAIN_DICTIONARY"
}

object DELTA_BINARY_PACKED extends Encoding {
  override val id: Int = 4
  override val value: String = "DELTA_BINARY_PACKED"
}

object DELTA_LENGTH_BYTE_ARRAY extends Encoding {
  override val id: Int = 5
  override val value: String = "DELTA_LENGTH_BYTE_ARRAY"
}

object DELTA_BYTE_ARRAY extends Encoding {
  override val id: Int = 6
  override val value: String = "DELTA_BYTE_ARRAY"
}

object RLE_DICTIONARY extends Encoding {
  override val id: Int = 7
  override val value: String = "RLE_DICTIONARY"
}