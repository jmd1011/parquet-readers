package main.scala.Parq

/**
  * Created by James on 8/1/2016.
  */
trait FilterPredicate {
  def accept[R](visitor: Visitor[R]): R

  trait Visitor[R] {
    def visit[T](): R
  }
}
