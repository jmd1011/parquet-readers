package main.scala.Parq

/**
  * Created by James on 7/7/2016.
  */
abstract class Converter {
  def isPrimitive: Boolean
  def asPrimitiveConverter(): PrimitiveConverter = throw new ClassCastException("Expected instance of primitive converter but got \"" + this.getClass.getName + "\" instead.")
  def asGroupConverter(): GroupConverter = throw new ClassCastException("Expected instance of group converter but got \"" + this.getClass.getName + "\" instead.")
}