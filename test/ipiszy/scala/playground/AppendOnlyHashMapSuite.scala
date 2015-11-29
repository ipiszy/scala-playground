package ipiszy.scala.playground

import java.util

import org.scalatest.FunSuite

import scala.collection.{mutable, JavaConversions}
import scala.runtime.RichInt

/**
  * Created by carolzhang on 11/28/15.
  */
class AppendOnlyHashMapSuite extends FunSuite {
  test("Initialization.") {
    var map = new AppendOnlyHashMap[Int, String]()
    assert(map.size === 0)
    map = new AppendOnlyHashMap[Int, String](4)
    assert(map.size === 0)

    intercept[IllegalArgumentException] {
      new AppendOnlyHashMap[Int, Int](-4)
    }
    intercept[IllegalArgumentException] {
      new AppendOnlyHashMap[Int, Int](2 << 29 + 1)
    }
  }

  test("Basic put and set.") {
    val map = new AppendOnlyHashMap[Int, String]()
    map.put(12, "ipiszy")
    map.put(0, "add")
    assert(map.size === 2)
    assert(map.get(12) === "ipiszy")
    assert(map.get(0) === "add")
    assert(map.get(1) === null.asInstanceOf[String])
  }

  test("Null key.") {
    val map = new AppendOnlyHashMap[Int, String]()
    assert(map.get(null.asInstanceOf[Int]) === null.asInstanceOf[String])
    map.put(null.asInstanceOf[Int], "")
    assert(map.get(null.asInstanceOf[Int]) === "")
    assert(map.size === 1)
  }

  test("Null value.") {
    val map = new AppendOnlyHashMap[Int, String]()
    map.put(null.asInstanceOf[Int], null.asInstanceOf[String])
    assert(map.get(null.asInstanceOf[Int]) === null.asInstanceOf[String])
    map.put(10, null.asInstanceOf[String])
    assert(map.get(10) === null.asInstanceOf[String])
    assert(map.size === 2)
  }

  test("Update value.") {
    val map = new AppendOnlyHashMap[Int, String]()
    map.put(null.asInstanceOf[Int], null.asInstanceOf[String])
    map.put(12, "ipiszy")
    map.put(null.asInstanceOf[Int], "ipiszy")
    map.put(12, "add")
    assert(map.get(null.asInstanceOf[Int]) === "ipiszy")
    assert(map.get(12) === "add")
    assert(map.size === 2)
  }

  test("Map grow.") {
    val map = new AppendOnlyHashMap[Int, Int](1)
    for (i <- 0 until 100) map.put(i, i)
    assert(map.size === 100)
    for (i <- 0 until 100) assert(map.get(i) === i)
  }

  ignore("Test null behavior of scala/java map.") {
    def testNull[K, V](map: mutable.Map[K, V]) = {
      map.put(null.asInstanceOf[K], null.asInstanceOf[V])
      print(map.getClass + ": ")
      for ((k, v) <- map) print(k, v)
      println
    }
    val map1 = new mutable.HashMap[Int, Int]()
    testNull(map1)
    for ((k, v) <- map1) print(k, v)
    map1.foreach(ele => print(ele._1, ele._2))
    println
    val map2 = new mutable.HashMap[String, String]()
    testNull(map2)
    for ((k, v) <- map2) print(k, v)
    println
    val map3 = JavaConversions.mapAsScalaMap(new util.HashMap[Int, Int]())
    testNull(map3)
    for ((k, v) <- map3) print(k, v)
    println
    testNull(JavaConversions.mapAsScalaMap(new util.HashMap[String, String]()))
  }

  test("Iterator.") {
    val map = new AppendOnlyHashMap[Integer, Integer]()
    for (i <- 0 until 10) map.put(i, i)
    map.put(null.asInstanceOf[Integer], -1)

    var foundNullKey = false
    val orderedMap = new util.TreeMap[Integer, Integer]()
    for ((k, v) <- map) {
      if (k == null) {
        foundNullKey = true
        assert(v === -1)
      } else {
        orderedMap.put(k, v)
      }
    }

    var i = 0
    for ((k, v) <- JavaConversions.mapAsScalaMap(orderedMap)) {
      assert(k === i)
      assert(v === i)
      i += 1
    }
  }
}
