package main.scala.Fauxquet.flare.metadata

import java.util.concurrent.ConcurrentHashMap

/**
  * Created by james on 1/31/17.
  */
class Canonicalizer[T] {
  var canonicals = new ConcurrentHashMap[T, T]()

  def canonicalize(value: T): T = {
    val canonical = canonicals.get(value)

    if (canonical == null) {
      val can = toCanonical(value)
      val x = canonicals.putIfAbsent(can, can)

      if (x == null) can
      else x
    } else {
      canonical
    }
  }

  def toCanonical(value: T): T = value
}
