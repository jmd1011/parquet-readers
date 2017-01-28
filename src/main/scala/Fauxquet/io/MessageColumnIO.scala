package main.scala.Fauxquet.io

import main.scala.Fauxquet.schema.MessageType

/**
  * Created by james on 1/28/17.
  */
class MessageColumnIO(messageType: MessageType, val validating: Boolean, val createdBy: String) extends GroupColumnIO(messageType, null, 0) {



  override def columnNames: List[Array[String]] = ???

  override def getLast: PrimitiveColumnIO = ???

  override def getFirst: PrimitiveColumnIO = ???
}
