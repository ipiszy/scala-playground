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
  println(null.equals(null))
}
