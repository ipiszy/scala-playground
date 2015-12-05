package ipiszy.scala.playground

import java.util
import java.util.ConcurrentModificationException

import io.Source
import scala.collection.mutable
import scala.util.Sorting

/**
  * Created by carolzhang on 11/28/15.
  */
class A {
  def helloWorld() {println("Hello world A")}
}

class B extends A {
  override def helloWorld {println("Hello world B")}
}

object Hello extends App {
  // Test polymorphism.
  val b = new B()
  b.helloWorld
  val a = b.asInstanceOf[A]
  a.helloWorld
  println(a.getClass)
  println(a.isInstanceOf[B])

  // Test eq and equals.
  println(a.eq(b))
  println(a.equals(null))
  println(null.eq(null))
  try {
    println(null.equals(null))
  } catch {
    case e: Exception => e.printStackTrace()
  }

  // Test () and update.
  val map1 = new scala.collection.mutable.HashMap[Int, Int]()
  map1(12) = 1200
  map1.update(34, 10)
  println(map1.mkString(", "))

  // Test map and flatMap.
  (1 to 9).map("*" * _).foreach(println(_))
  val source = Source.fromFile("/Users/carolzhang/ipiszy")
  val textArr = source.getLines().toArray
  println(textArr.map(_.split(' ')).mkString(", "))
  val words = textArr.flatMap(_.split(' '))
  val uniqueWords = new mutable.HashSet[String]()
  words.foreach(uniqueWords += _)
  println(uniqueWords.toArray.sorted(math.Ordering.String).mkString(", "))

  // Test currying
  def currying(a: Int)(b: Int)(c: Int)(d: Int) = a * b * c * d
  val c1 = currying(3)(1) _
  println(c1(5)(6))
  val c2 = c1(4)
  println(c2(3))
  def cur(a:Int, b: Int, c: Int, d: Int) = a*b*c*d
  val c3 = (b:Int, d:Int) => cur(3, b, 1, d)

  // Test container
  val list1 = new util.LinkedList[Int]()
  list1.add(1)
  list1.add(5)
  list1.add(10)
  val itList = list1.iterator()
  println(itList.next())
  list1.add(20)
  try {
    println(itList.next())
  } catch {
    case ex: ConcurrentModificationException => ex.printStackTrace()
  }

  val pairs = Array(("a", 5, 2), ("c", 3, 1), ("b", 1, 3))
  Sorting.quickSort(pairs)(Ordering[(Int, String)].on(x => (x._3, x._1)))
  println(pairs.mkString(", "))
}
