package main.scala.Fauxquet.FauxquetObjs

/**
  * Created by james on 8/9/16.
  */
object FieldRepetitionTypeManager {
  def getFieldRepetitionTypeById(id: Int): FieldRepetitionType = id match {
    case 0 => FieldRepetitionType(0, "REQUIRED")
    case 1 => FieldRepetitionType(1, "OPTIONAL")
    case 2 => FieldRepetitionType(2, "REPEATED")
    case _ => null
  }
}

case class FieldRepetitionType(id: Int, value: String)