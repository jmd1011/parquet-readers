package main.scala.Parq

import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.Binary

/**
  * Created by James on 7/7/2016.
  */
abstract class PrimitiveConverter extends Converter {
  def this() = this
  def isPrimitive = true
  override def asPrimitiveConverter(): PrimitiveConverter = this
  def hasDictionarySupport = false

  def throwException() = throw new UnsupportedOperationException(this.getClass.getName)

  def setDictionary(dictionary: Dictionary) = throwException()
  def addValueFromDictionary(dictionaryId: Int) = throwException()
  def addBinary(value: Binary) = throwException()
  def addBoolean(value: Boolean) = throwException()
  def addDouble(value: Boolean) = throwException()
  def addFloat(value: Boolean) = throwException()
  def addInt(value: Boolean) = throwException()
  def addLong(value: Boolean) = throwException()
}
