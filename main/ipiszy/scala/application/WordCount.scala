package ipiszy.scala.application

import ipiszy.scala.mapreduce._

class WordSplitMapper extends Mapper {
  override def map(input: Array[Byte]): Array[(Array[Byte], Array[Byte])] = {
    for (w <- new String(input).split("\\s+")) yield (w.getBytes(), "1".getBytes())
  }
}

class WordSumReducer extends Reducer {
  override def reduce(key: Array[Byte], values: Array[Array[Byte]]):
  (Array[Byte], Array[Byte]) = (key, values.length.toString.getBytes())
}

/**
  * Created by carolzhang on 12/3/15.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val spec = new MapReduceSpec(
      new MapReduceInputSpec("/tmp/input/*.txt", FileFormat.TEXT),
      new MapReduceOutputSpec("/tmp/output/count", FileFormat.TEXT),
      "ipiszy.scala.application.WordSplitMapper", 1,
      "ipiszy.scala.application.WordSumReducer", 1
    )
    val controller = new MapReduceController(spec)
    controller.start()
  }
}
