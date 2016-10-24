package main.scala.Parq

import org.apache.parquet.schema.{GroupType, MessageType, PrimitiveType, TypeVisitor}

/**
  * Created by James on 7/28/2016.
  */
class ColumnIOFactory(createdBy: String) {
  def getColumnIO(requestedSchema: MessageType, fileSchema: MessageType): MessageColumnIO = {
    val visitor = new ColumnIOCreatorVisitor
    fileSchema.accept(visitor)

    visitor.columnIO
  }

  class ColumnIOCreatorVisitor extends TypeVisitor {
    var columnIO: MessageColumnIO = _

    override def visit(groupType: GroupType): Unit = ???

    override def visit(messageType: MessageType): Unit = ???

    override def visit(primitiveType: PrimitiveType): Unit = ???
  }
}
