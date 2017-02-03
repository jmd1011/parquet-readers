package main.scala.Fauxquet.page

import main.scala.Fauxquet.FauxquetObjs.Encoding
import main.scala.Fauxquet.bytes.BytesInput.BytesInput

/**
  * Created by james on 1/26/17.
  */
class DictionaryPage(val bytes: BytesInput, val dictionarySize: Int, val encoding: Encoding) extends Page(bytes.size.asInstanceOf[Int], bytes.size.asInstanceOf[Int]) {

}
