package main.scala.Fauxquet.schema

/**
  * Created by james on 1/28/17.
  */
trait TypeVisitor {
  def visit(groupType: GroupType)
  def visit(messageType: MessageType)
  def visit(primitiveType: PrimitiveType)
}
