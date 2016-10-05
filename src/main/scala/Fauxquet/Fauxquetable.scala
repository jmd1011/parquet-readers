package main.scala.Fauxquet

import main.scala.Fauxquet.FauxquetObjs.{KeyValue, TField}

/**
  * Created by james on 8/5/16.
  */

trait Fauxquetable {
  var keyValueMetadata: List[KeyValue] = Nil
  def className: String

  def read(arr: SeekableArray[Byte]): Unit = {
    //println(s"Starting read of $className, arr.pos = ${arr.pos}.")

    FauxquetDecoder readStructBegin()

    var keepGoing = true
    var id = 0

    while (keepGoing) {
      val field = FauxquetDecoder readFieldBegin (arr, id)
      id = field id

      field match {
        case TField(_, 0, _) =>
          FauxquetDecoder readStructEnd field.id
          validate()
          keepGoing = false
          //println(s"Finished read of $className, arr.pos = ${arr.pos}.")
        case _ => doMatch(field, arr)
      }
    }
  }

  def write() = {
    validate()
  }

  def validate()

  def doMatch(field: TField, arr: SeekableArray[Byte])
}