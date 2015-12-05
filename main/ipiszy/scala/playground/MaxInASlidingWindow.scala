package ipiszy.scala.playground

import java.util


/**
  * Created by carolzhang on 11/29/15.
  */
object MaxInASlidingWindow extends App {
  def maxInASlidingWindow(arr: Array[Int], windowSize: Int): Array[Int] = {
    require(windowSize >= 1,
      "The windowSize must be greater than or equal to 1. " +
      "The current windowSize: " + windowSize)
    val result = new Array[Int](arr.length)
    val history = new util.LinkedList[Int]()
    for (i <- arr.indices) {
      if (!history.isEmpty && history.getFirst + windowSize == i) history.removeFirst()
      val it = history.descendingIterator()
      while (it.hasNext && arr(it.next()) <= arr(i)) {
        it.remove()
      }
      history.add(i)
      result(i) = arr(history.getFirst)
    }
    result
  }

  println(maxInASlidingWindow(Array(3,6,1,0,5), 1).mkString(", "))
  println(maxInASlidingWindow(Array(3,6,1,0,5), 2).mkString(", "))
}
