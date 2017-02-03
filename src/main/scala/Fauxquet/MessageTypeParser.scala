package main.scala.Fauxquet

import java.util.StringTokenizer

import main.scala.Fauxquet.schema.OriginalType.OriginalType
import main.scala.Fauxquet.schema._

/**
  * Created by james on 2/3/17.
  */
object MessageTypeParser {
  def parse(schemaStr: String): MessageType = {
    val st = new Tokenizer(schemaStr)
    val t = st.nextToken

    if (t != "message") {
      throw new Error("Invalid schema")
    }

    val name = st.nextToken
    val groups = readGroupTypeFields(st.nextToken, st)

    new MessageType(null, name, groups)
  }

  def readGroupTypeFields(t: String, st: Tokenizer): List[BaseType] = {
    var token: String = st.nextToken

    var types = List[BaseType]()

    while (token != "}") {
      types :+= readType(token, st)

      token = st.nextToken
    }

    types
  }

  def readType(t: String, st: Tokenizer): BaseType = {
    var token = t

    val rep = asRepetition(t)
    val Type = st.nextToken
    val name = st.nextToken

    token = st.nextToken

    val originalType: OriginalType = {
      if (token.equalsIgnoreCase("(")) {
        val x = OriginalType.getOriginalTypeByString(st.nextToken)

        if (st.nextToken != ")") throw new Error("I have bad parens")

        token = st.nextToken

        x
      }
      else
        null
    }

    if (Type.equalsIgnoreCase("group")) {
      val e1 = readGroupTypeFields(token, st)
      new GroupType(rep, name, originalType, e1)
    } else {
      val e = asPrimitive(Type)
      new PrimitiveType(rep, e, 0, name, originalType, null, null)
    }
  }

  def asPrimitive(Type: String): PrimitiveTypeName = PrimitiveTypeName.getPrimitiveTypeNameByString(Type.toUpperCase)
  def asRepetition(t: String): Repetition = RepetitionManager.getRepetitionByName(t.toUpperCase)

  private class Tokenizer(schemaStr: String) {
    val st = new StringTokenizer(schemaStr, " ;{}()\n\t", true)
    var line: Int = 0
    var buffer: StringBuffer = new StringBuffer()

    def nextToken: String = {
      while (true) {
        if (this.st.hasMoreTokens) {
          val t = st.nextToken()

          if (t.equals("\n")) {
            line += 1
            this.buffer.setLength(0)
          } else {
           this.buffer.append(t)
          }

          if (!isWhitespace(t)) {
            return t
          }
        }

        throw new Error("Unexpected end of schema")
      }
    }

    def isWhitespace(str: String) = str == " " || str == "\n" || str == "\t"
  }
}
