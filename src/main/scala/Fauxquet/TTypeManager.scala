package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
object TTypeManager {
  def getType(i: Int): TType = {
    i match {
      case 0 => TType(0, "BOOLEAN")
      case 1 => TType(1, "INT32")
      case 2 => TType(2, "INT64")
      case 3 => TType(3, "INT96")
      case 4 => TType(4, "FLOAT")
      case 5 => TType(5, "DOUBLE")
      case 6 => TType(6, "BYTE_ARRAY")
      case 7 => TType(7, "FIXED_LEN_BYTE_ARRAY")
      case _ => null
    }
  }
}
