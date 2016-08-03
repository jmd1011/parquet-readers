package main.scala.Parq

import java.io.{DataInput, DataOutput}
import java.util.Map.Entry
import java.util.Properties

/**
  * Created by James on 7/11/2016.
  */
class Configuration extends Iterable[Entry[String, String]] with Writable {
  var properties: Properties = null

  override def write(out: DataOutput): Unit = {

  }

  override def readFields(in: DataInput): Unit = {

  }

  override def iterator: Iterator[Entry[String, String]] = ???

  def substituteVars(property: String): String = ???

  def get(name: String): String = {
    this.substituteVars(this.getProps.getProperty(name))
  }

  def getProps: Properties = {
    if (this.properties == null) {
      this.properties = new Properties()
      //this.loadResources(this.properties, this.resources, this.quietmode)
    }

    this.properties
  }

  def getHexDigits(string: String): String = ???

  def getInt(name: String, defaultValue: Int): Int = ??? //{
//    val str = this.get(name)
//
//    if (null == str) defaultValue
//    else {
//      try {
//        val e = this.getHexDigits(str)
//        if (e != null) Integer.parseInt(e, 16)
//        else Integer.parseInt(str)
//      } catch {
//        case NumberFormatException => throw new Error("Unable to parse Int")
//      }
//    }
//  }
}

trait Writable {
  def write(out: DataOutput)
  def readFields(in: DataInput)
}