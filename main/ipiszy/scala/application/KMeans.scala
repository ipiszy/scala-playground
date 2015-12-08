package ipiszy.scala.application

import java.io._
import java.nio.file.{Files, Paths}
import java.util.logging.Logger

import ipiszy.scala.mapreduce._

import scala.io.Source

/**
  * Created by carolzhang on 12/6/15.
  */

class KMeansMapper extends Mapper {
  val kPoints = KMeans.readKPointsFromIteration(KMeans.currentIteration - 1)
  val byteOS = new ByteArrayOutputStream()
  val objOS = new ObjectOutputStream(byteOS)
  val logger = Logger.getLogger(this.getClass.toString)
  logger.info("kPoints: " + kPoints.map(_.mkString).mkString("\n"))

  private def distance(p1: Array[Double], p2: Array[Double]): Double = {
    p1.zip(p2).foldLeft(0.0)((x, y) => x + math.pow(y._1 - y._2, 2))
  }

  private def pointToBytes(p: Array[Double]): Array[Byte] = {
    objOS.writeObject(p)
    objOS.flush()
    val result = byteOS.toByteArray
    objOS.reset()
    result
  }

  override def map(input: Array[Byte]): Array[(Array[Byte], Array[Byte])] = {
    val currentP = new String(input).split("\\s+", KMeans.D).map(_.toDouble)
    if (currentP.length != KMeans.D) null
    else {
      var minPointIdx = 0
      var minDistance = Double.MaxValue
      kPoints.indices.foreach(idx => {
        val dis = distance(kPoints(idx), currentP)
        if (dis < minDistance) {
          minPointIdx = idx
          minDistance = dis
        }
      })
      Array((minPointIdx.toString.getBytes, pointToBytes(currentP)))
    }
  }
}

class KMeansReducer extends Reducer {
  private def bytesToPoint(bytes: Array[Byte]): Array[Double] = {
    val byteIS = new ByteArrayInputStream(bytes)
    val objIS = new ObjectInputStream(byteIS)
    objIS.readObject().asInstanceOf[Array[Double]]
  }

  override def reduce(key: Array[Byte], values: Array[Array[Byte]])
      : (Array[Byte], Array[Byte]) = {
    var count = 0
    val sumP = Array.ofDim[Double](KMeans.D)
    for (value <- values) {
      val point = bytesToPoint(value)
      assert(sumP.length == point.length)
      for (i <- sumP.indices) sumP(i) += point(i)
      count += 1
    }
    sumP.transform(_ / count)
    (sumP.mkString(" ").getBytes, count.toString.getBytes())
  }
}

object KMeans {
  // Assume that there are n points, each point is represented as a
  // d-dimension vector, and we want to calculate k means.
  val D = 20
  val K = 10
  // Define the input and output.
  val INPUT_PATH = "/tmp/kmeans/input/input_small.txt"
  val OUTPUT_PATH = "/tmp/kmeans/output/output"
  val HISTORY_OUTPUT = "/tmp/kmeans/history/"
  val MAX_ITERATION = 100
  var currentIteration = 0
  val logger = Logger.getLogger(this.getClass.toString)

  private def readKPoints(glob: String): Array[Array[Double]] = {
    val result = Array.ofDim[Double](K, D)
    val fileIt = {
      val filePattern = new File(glob)
      val dir = filePattern.getParent
      val name = filePattern.getName
      Files.newDirectoryStream(Paths.get(dir), name).iterator()
    }
    var idx = 0
    while (fileIt.hasNext) {
      val source = Source.fromFile(fileIt.next().toAbsolutePath.toString)
      val lineIt = source.getLines()
      for (line <- lineIt) {
        val nums = line.split("\\s+", D + 1)
        for (i <- 0 until D) {
          result(idx)(i) = nums(i).toDouble
        }
        idx += 1
        if (idx == K) {
          source.close()
          return result
        }
      }
      source.close()
    }
    result
  }

  private def genKInitialPoints(): Array[Array[Double]] = {
    val result = readKPoints(INPUT_PATH)
    if (result.length != K)
      throw new IllegalArgumentException("Cannot find K initial points!")
    result
  }

  private def startKMeans(): Unit = {
    val spec = new MapReduceSpec(
      new MapReduceInputSpec(INPUT_PATH, FileFormat.TEXT),
      new MapReduceOutputSpec(HISTORY_OUTPUT + currentIteration,
        FileFormat.TEXT),
      "ipiszy.scala.application.KMeansMapper", 10,
      "ipiszy.scala.application.KMeansReducer", 5
    )
    val controller = new MapReduceController(spec)
    controller.start()
  }

  def readKPointsFromIteration(iteration: Int): Array[Array[Double]] = {
    val result = readKPoints(HISTORY_OUTPUT + iteration + "-*")
    if (result.length != K) {
      throw new RuntimeException(
        "Cannot find K points from iteration " + iteration + ".")
    }
    result
  }

  private def areSame(prev: Array[Array[Double]], current: Array[Array[Double]])
      : Boolean = {
    if (prev.length != current.length) return false
    prev.sortBy(_.mkString(",")).sameElements(current.sortBy(_.mkString(",")))
  }

  private def writeKPointsToOutput(kPoints: Array[Array[Double]],
                                   path: String): Unit = {
    val writer = new PrintWriter(path)
    for (point <- kPoints) writer.write(point.mkString(" ") + "\n")
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    logger.info("Start KMeans.")
    // 1. Find k initial points.
    logger.info("Select the first K points.")
    var prevK = genKInitialPoints()
    writeKPointsToOutput(prevK, HISTORY_OUTPUT + "0-0")
    // 2. Try to find k means.
    currentIteration = 1
    while (currentIteration <= MAX_ITERATION) {
      logger.info("Start iteration #" + currentIteration + ".")
      startKMeans()

      val currentK = readKPointsFromIteration(currentIteration)
      if (areSame(prevK, currentK)) {
        writeKPointsToOutput(currentK, OUTPUT_PATH)
        logger.info("Found K points @iteration " + currentIteration + ".")
        return
      }
      prevK = currentK
      currentIteration += 1
    }

    writeKPointsToOutput(prevK, OUTPUT_PATH)
    logger.info("K points haven't been converged after " + MAX_ITERATION +
      " iterations.")
  }
}
