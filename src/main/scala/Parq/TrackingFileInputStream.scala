package main.scala.Parq

import java.io.{File, FileInputStream}

/**
  * Created by james on 8/3/16.
  */
class TrackingFileInputStream(f: File) extends FileInputStream(f) {
  override def read(): Int = {
    val res = super.read()
    res
  }
}
