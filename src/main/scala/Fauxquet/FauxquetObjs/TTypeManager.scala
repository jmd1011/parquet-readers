package main.scala.Fauxquet.FauxquetObjs

/**
  * Created by james on 8/9/16.
  */
object TTypeManager {
  def getType(i: Int): TType = {
    i match {
      case 0 => BOOLEAN
      case 1 => INT32
      case 2 => INT64
      case 3 => INT96
      case 4 => FLOAT
      case 5 => DOUBLE
      case 6 => BYTE_ARRAY
      case 7 => FIXED_LEN_BYTE_ARRAY
      case _ => null
    }
  }

  def trySomethingCool(tType: TType, x: Any, f: (Any, Int, Int) => Unit): Unit = {
    tType match {
      case BOOLEAN => f(x.asInstanceOf[Boolean], 0, 1)
      case INT32 => f(x.asInstanceOf[Int], 0, 1)
      case INT64 => f(x.asInstanceOf[Long], 0, 1)
      case INT96 => throw new Error("Int96 unsupported") /*f(x.asInstanceOf[INT96], 0, 1)*/
      case FLOAT => f(x.asInstanceOf[Float], 0, 1)
      case DOUBLE => f(x.asInstanceOf[Double], 0, 1)
      case BYTE_ARRAY => f(x.asInstanceOf[Array[Byte]], 0, 1)
      case FIXED_LEN_BYTE_ARRAY => f(x.asInstanceOf[Array[Byte]], 0, 1)
    }
  }
}

trait TType {
  val id: Int
  val value: String
}

object BOOLEAN extends TType {
  override val id: Int = 0
  override val value: String = "BOOLEAN"
}

object INT32 extends TType {
  override val id: Int = 1
  override val value: String = "INT32"
}

object INT64 extends TType {
  override val id: Int = 2
  override val value: String = "INT64"
}

object INT96 extends TType {
  override val id: Int = 3
  override val value: String = "INT96"
}

object FLOAT extends TType {
  override val id: Int = 4
  override val value: String = "FLOAT"
}

object DOUBLE extends TType {
  override val id: Int = 5
  override val value: String = "DOUBLE"
}

object BYTE_ARRAY extends TType {
  override val id: Int = 6
  override val value: String = "BYTE_ARRAY"
}

object FIXED_LEN_BYTE_ARRAY extends TType {
  override val id: Int = 7
  override val value: String = "FIXED_LEN_BYTE_ARRAY"
}