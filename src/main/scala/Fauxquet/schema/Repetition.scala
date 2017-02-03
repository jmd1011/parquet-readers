package main.scala.Fauxquet.schema

/**
  * Created by james on 1/28/17.
  * not sure why we need this AND FieldRepetitionType
  */
trait Repetition {
  val name: String
  def isMoreRestrictiveThan(other: Repetition): Boolean
}

object REQUIRED extends Repetition {
  override val name: String = "REQUIRED"
  override def isMoreRestrictiveThan(other: Repetition): Boolean = other != REQUIRED
}

object OPTIONAL extends Repetition {
  override val name: String = "OPTIONAL"
  override def isMoreRestrictiveThan(other: Repetition): Boolean = other == REQUIRED
}

object REPEATED extends Repetition {
  override val name: String = "REPEATED"
  override def isMoreRestrictiveThan(other: Repetition): Boolean = false
}

object RepetitionManager {
  def getRepetitionByName(name: String): Repetition = name match {
    case "REQUIRED" => REQUIRED
    case "OPTIONAL" => OPTIONAL
    case "REPEATED" => REPEATED
    case _ => throw new Error("Unable to match")
  }
}