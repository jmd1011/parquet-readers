package main.scala.Fauxquet.FauxquetObjs

/**
  * Created by james on 8/9/16.
  */
object ConvertedTypeManager {
  def getConvertedTypeById(id: Int): ConvertedType = id match {
    case 0 => UTF8
    case 1 => MAP
    case 2 => MAP_KEY_VALUE
    case 3 => LIST
    case 4 => ENUM
    case 5 => DECIMAL
    case 6 => DATE
    case 7 => TIME_MILLIS
    //uhhhhhhhh they missed one
    case 9 => TIMESTAMP_MILLIS
    //man these guys sucked at counting
    case 11 => UINT_8
    case 12 => UINT_16
    case 13 => UINT_32
    case 14 => UINT_64
    case 15 => INT_8
    case 16 => INT_16
    case 17 => INT_32
    case 18 => INT_64
    case 19 => JSON
    case 20 => BSON
    case 21 => INTERVAL
    case _ => null
  }
}

trait ConvertedType {
  val id: Int
  val value: String
}

object UTF8 extends ConvertedType {
  override val id: Int = 0
  override val value: String = "UTF8"
}

object MAP extends ConvertedType {
  override val id: Int = 1
  override val value: String = "MAP"
}

object MAP_KEY_VALUE extends ConvertedType {
  override val id: Int = 2
  override val value: String = "MAP_KEY_VALUE"
}

object LIST extends ConvertedType {
  override val id: Int = 3
  override val value: String = "LIST"
}

object ENUM extends ConvertedType {
  override val id: Int = 4
  override val value: String = "ENUM"
}

object DECIMAL extends ConvertedType {
  override val id: Int = 5
  override val value: String = "DECIMAL"
}

object DATE extends ConvertedType {
  override val id: Int = 6
  override val value: String = "DATE"
}

object TIME_MILLIS extends ConvertedType {
  override val id: Int = 7
  override val value: String = "TIME_MILLIS"
}

object TIMESTAMP_MILLIS extends ConvertedType {
  override val id: Int = 9
  override val value: String = "TIMESTAMP_MILLIS"
}

object UINT_8 extends ConvertedType {
  override val id: Int = 11
  override val value: String = "UINT_8"
}

object UINT_16 extends ConvertedType {
  override val id: Int = 12
  override val value: String = "UINT_16"
}

object UINT_32 extends ConvertedType {
  override val id: Int = 13
  override val value: String = "UINT_32"
}

object UINT_64 extends ConvertedType {
  override val id: Int = 14
  override val value: String = "UINT_64"
}

object INT_8 extends ConvertedType {
  override val id: Int = 15
  override val value: String = "INT_8"
}

object INT_16 extends ConvertedType {
  override val id: Int = 16
  override val value: String = "INT_16"
}

object INT_32 extends ConvertedType {
  override val id: Int = 17
  override val value: String = "INT_32"
}

object INT_64 extends ConvertedType {
  override val id: Int = 18
  override val value: String = "INT_64"
}

object JSON extends ConvertedType {
  override val id: Int = 19
  override val value: String = "JSON"
}

object BSON extends ConvertedType {
  override val id: Int = 20
  override val value: String = "BSON"
}

object INTERVAL extends ConvertedType {
  override val id: Int = 21
  override val value: String = "INTERVAL"
}