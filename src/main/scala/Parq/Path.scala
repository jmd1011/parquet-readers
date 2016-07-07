package main.scala.Parq

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
  * Created by James on 7/7/2016.
  */
class Path extends Comparable[Path] {
  val SEPARATOR = "/"
  val CUR_DIR = "."
  val WINDOWS = System.getProperty("os.name").startsWith("Windows")
  var uri: URI = null

  def this(pathString: String) = {
    this()

    val str: String = {
      if (hasWindowsDrive(pathString, slashed = false)) "/" + pathString else pathString
    }

    val colonPos = pathString indexOf 58
    val slashPos = pathString indexOf 47

    val (scheme, start): (String, Int) = {
      if (colonPos != -1 && (slashPos == -1 || colonPos < slashPos)) (str substring(0, colonPos), colonPos + 1)
      else ("", 0)
    }

    val (authority, start2): (String, Int) = {
      if (str.startsWith("//", start) && str.length - start > 2) {
        val path = str indexOf(47, start + 2)
        val authEnd = if (path > 0) path else str length

        (str substring(start + 2, authEnd), authEnd)
      } else ("", start)
    }

    val path1 = str substring(start2, str length)
    this.initialize(scheme, authority, path1, null)
  }

  def normalizePath(path: String) = {
    var res = path

    if (res.indexOf("//") != -1) res = res.replace("//", "/")
    if (res.indexOf("\\") != -1) res = res.replace("\\", "/")

    val minLength = if (hasWindowsDrive(res, slashed = true)) 4 else 1
    if (res.length > minLength && res.endsWith("/")) res = res substring(0, res.length - 1)

    res
  }

  def initialize(scheme: String, authority: String, path: String, fragment: String) = {
    this.uri = new URI(scheme, authority, normalizePath(path), null, fragment).normalize()
  }

  def hasWindowsDrive(path: String, slashed: Boolean): Boolean = {
    if (!WINDOWS) false
    val start = if (slashed) 1 else 0
    val first = path.charAt(start)
    path.length() >= start + 2 && (!slashed || path.charAt(0) == 47) && path.charAt(start + 1) == 58 && (first >= 65 && first <= 90 || first >= 97 && first <= 122)
  }

  def checkPathArg(path: String) = if (path == null) throw new IllegalArgumentException("Can not create a Path from a null string") else if (path.length == 0) throw new IllegalArgumentException("Can not create a Path from an empty string")

  override def compareTo(o: Path): Int = 1

  def getFileSystem(conf: Configuration) = FileSystem.get(uri, conf)
}
