package main.scala.Parq

/**
  * Created by James on 8/1/2016.
  */
class FilterCompat {
  def get(filterPredicate: FilterPredicate): Filter = {
    //val collapsedPredicate = LogicalInverseRewriter.rewrite(filterPredicate) ??

    new FilterPredicateCompat(filterPredicate)
  }
}

trait Filter {
  def accept[T](visitor: Visitor[T]): T
}

class FilterPredicateCompat(val filterPredicate: FilterPredicate) extends Filter {
  def accept[R](visitor: Visitor[R]): R = visitor visit this
  def getFilterPredicate = filterPredicate
}

trait Visitor[T] {
  def visit(filterPredicateCompat: FilterPredicateCompat): T
}
