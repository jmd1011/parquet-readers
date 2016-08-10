package main.scala.Fauxquet

/**
  * Created by james on 8/5/16.
  */

trait Fauxquetable {
  var keyValueMetadata: List[KeyValue] = Nil

  def read(arr: SeekableArray[Byte]): Unit = {
    FauxquetDecoder readStructBegin()

    var keepGoing = true

    while (keepGoing) {
      val field = FauxquetDecoder readFieldBegin arr

      field match {
        case TField(_, 0, _) =>
          FauxquetDecoder readStructEnd field.id
          validate()
          keepGoing = false
        case _ => doMatch(field, arr)
      }
    }
  }

  def write()

  def validate()

  def doMatch(field: TField, arr: SeekableArray[Byte])
}