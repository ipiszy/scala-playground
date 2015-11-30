package ipiszy.scala.playground

import org.scalatest.FunSuite

/**
  * Created by carolzhang on 11/29/15.
  */
class MaxInASlidingWindowSuite extends FunSuite {
  test("Empty array.") {
    val result = MaxInASlidingWindow.maxInASlidingWindow(new Array[Int](0), 5)
    assert(result.isEmpty)
  }

  test("Window size is not positive.") {
    intercept[IllegalArgumentException] {
      MaxInASlidingWindow.maxInASlidingWindow(new Array[Int](0), -1)
    }
    intercept[IllegalArgumentException] {
      MaxInASlidingWindow.maxInASlidingWindow(new Array[Int](0), 0)
    }
  }

  test("Regular test.") {
    assert(MaxInASlidingWindow.maxInASlidingWindow(
      Array(1, 3, 5, 7, 6, 2, 1), 1) === Array(1, 3, 5, 7, 6, 2, 1))
    assert(MaxInASlidingWindow.maxInASlidingWindow(
      Array(1, 3, 5, 7, 6, 2, 1), 2) === Array(1, 3, 5, 7, 7, 6, 2))
    assert(MaxInASlidingWindow.maxInASlidingWindow(
      Array(1, 3, 5, 7, 6, 2, 1), 5) === Array(1, 3, 5, 7, 7, 7, 7))
    assert(MaxInASlidingWindow.maxInASlidingWindow(
      Array(1, 5, 2, 4, 3, 7, 3), 3) === Array(1, 5, 5, 5, 4, 7, 7))
  }
}
