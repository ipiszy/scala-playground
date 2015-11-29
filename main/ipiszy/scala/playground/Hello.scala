package ipiszy.scala.playground

/**
  * Created by carolzhang on 11/28/15.
  */
class A {
  def HelloWorld {println("Hello world A")}
}

class B extends A {
  override def HelloWorld {println("Hello world B")}
}

object Hello extends App {
  val b = new B()
  b.HelloWorld
  val a = b.asInstanceOf[A]
  a.HelloWorld
  println(a.getClass)
  println(a.isInstanceOf[B])
  println(a.eq(b))
  println(a.equals(null))
  println(null.eq(null))
  try {
    println(null.equals(null))
  } catch {
    case e: Exception => e.printStackTrace()
  }

  val map1 = new scala.collection.mutable.HashMap[Int, Int]()
  map1(12) = 1200
  map1.update(34, 10)
  println(map1.mkString(", "))
}
