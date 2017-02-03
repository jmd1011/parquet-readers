package main.scala.Fauxquet.FauxquetObjs

/**
  * Created by james on 8/9/16.
  */
object FieldRepetitionTypeManager {
  def getFieldRepetitionTypeById(id: Int): FieldRepetitionType = id match {
    case 0 => REQUIRED
    case 1 => OPTIONAL
    case 2 => REPEATED
    case _ => null
  }

  def getFieldRepetitionTypeByName(value: String): FieldRepetitionType = value match {
    case "REQUIRED" => REQUIRED
    case "OPTIONAL" => OPTIONAL
    case "REPEATED" => REPEATED
    case _ => null
  }
}

trait FieldRepetitionType {
  val id: Int
  val value: String
}

object REQUIRED extends FieldRepetitionType {
  override val id: Int = 0
  override val value: String = "REQUIRED"
}

object OPTIONAL extends FieldRepetitionType {
  override val id: Int = 1
  override val value: String = "OPTIONAL"
}

object REPEATED extends FieldRepetitionType {
  override val id: Int = 2
  override val value: String = "REPEATED"
}

case class FieldRepetitionTypeMatch(id: Int, value: String)