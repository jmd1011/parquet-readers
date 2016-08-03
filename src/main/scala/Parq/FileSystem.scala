package main.scala.Parq

/**
  * Created by James on 8/2/2016.
  */
abstract class FileSystem {
  def open(path: Path, size: Int): FSDataInputStream
  def open(path: Path, configuration: Configuration): FSDataInputStream = open(path, configuration.getInt("io.file.buffer.size", 4096))
}
