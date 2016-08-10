package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
object PageTypeManager {
  def getPageTypeById(id: Int): PageType = id match {
    case 0 => PageType(0, "DATA_PAGE")
    case 1 => PageType(1, "INDEX_PAGE")
    case 2 => PageType(2, "DICTIONARy_PAGE")
    case 3 => PageType(3, "DATA_PAGE_V2")
    case _ => null
  }
}
