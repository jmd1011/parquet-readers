package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
object ConvertedTypeManager {
  def getConvertedTypeById(id: Int): ConvertedType = id match {
    case 0 => ConvertedType(0, "UTF8")
    case 1 => ConvertedType(1, "MAP")
    case 2 => ConvertedType(2, "MAP_KEY_VALUE")
    case 3 => ConvertedType(3, "LIST")
    case 4 => ConvertedType(4, "ENUM")
    case 5 => ConvertedType(5, "DECIMAL")
    case 6 => ConvertedType(6, "DATE")
    case 7 => ConvertedType(7, "TIME_MILLIS")
      //uhhhhhhhh they missed one
    case 9 => ConvertedType(9, "TIMESTAMP_MILLIS")
      //man these guys sucked at counting
    case 11 => ConvertedType(11, "UINT_8")
    case 12 => ConvertedType(12, "UINT_16")
    case 13 => ConvertedType(13, "UINT_32")
    case 14 => ConvertedType(14, "UINT_64")
    case 15 => ConvertedType(15, "INT_8")
    case 16 => ConvertedType(16, "INT_16")
    case 17 => ConvertedType(17, "INT_32")
    case 18 => ConvertedType(18, "INT_64")
    case 19 => ConvertedType(19, "JSON")
    case 20 => ConvertedType(20, "BSON")
    case 21 => ConvertedType(21, "INTERVAL")
    case _ => null
  }
}

case class ConvertedType(id: Int, value: String)