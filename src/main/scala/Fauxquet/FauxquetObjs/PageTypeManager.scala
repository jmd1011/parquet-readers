package main.scala.Fauxquet.FauxquetObjs

/**
  * Created by james on 8/9/16.
  */
object PageTypeManager {
  def getPageTypeById(id: Int): PageType = id match {
    case 0 => DATA_PAGE
    case 1 => INDEX_PAGE
    case 2 => DICTIONARY_PAGE
    case 3 => DATA_PAGE_V2
    case _ => null
  }
}

trait PageType {
  val id: Int
  val value: String
}

object DATA_PAGE extends PageType {
  override val id: Int = 0
  override val value: String = "DATA_PAGE"
}

object INDEX_PAGE extends PageType {
  override val id: Int = 1
  override val value: String = "INDEX_PAGE"
}

object DICTIONARY_PAGE extends PageType {
  override val id: Int = 2
  override val value: String = "DICTIONARY_PAGE"
}

object DATA_PAGE_V2 extends PageType {
  override val id: Int = 3
  override val value: String = "DATA_PAGE_V2"
}