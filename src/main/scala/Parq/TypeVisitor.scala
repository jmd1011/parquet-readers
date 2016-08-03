package main.scala.Parq

import org.apache.parquet.schema.PrimitiveType

/**
  * Created by James on 8/1/2016.
  */
trait TypeVisitor {
  def visit(groupType: GroupType)
  def visit(messageType: MessageType)
  def visit(primitiveType: PrimitiveType)
}
