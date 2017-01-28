package main.scala.Fauxquet.schema

/**
  * Created by james on 1/28/17.
  * not sure why we need this AND FieldRepetitionType
  */
trait Repetition {
  def isMoreRestrictiveThan(other: Repetition): Boolean
}

object REQUIRED extends Repetition {
  override def isMoreRestrictiveThan(other: Repetition): Boolean = other != REQUIRED
}

object OPTIONAL extends Repetition {
  override def isMoreRestrictiveThan(other: Repetition): Boolean = other == REQUIRED
}

object REPEATED extends Repetition {
  override def isMoreRestrictiveThan(other: Repetition): Boolean = false
}