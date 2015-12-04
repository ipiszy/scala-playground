package ipiszy.scala.mapreduce

import scala.collection.mutable.ArrayBuffer

/**
  * Created by carolzhang on 11/30/15.
  */
trait Sink {
  def writeKV(kv: (Array[Byte], Array[Byte])): Unit
  def close(): Unit
}

object Sink {
  def newFileSink(fileOutputSpec: FileOutputSpec): Sink = {
    fileOutputSpec.fileFormat match {
      case FileFormat.TEXT => {
        new TextFileSink(fileOutputSpec.filePrefix,
                         fileOutputSpec.currentInstanceId,
                         fileOutputSpec.outputInstanceNum)
      }
    }
  }

  def newSink(outputSpec: OutputSpec): Sink = {
    val sinks = new ArrayBuffer[Sink]
    outputSpec.getKind match {
      case IOKind.FILE => {
        outputSpec.fileOutput.foreach(sinks += newFileSink(_))
      }
      case IOKind.MEMORY => {
        val memSpec = outputSpec.memOutputSpec
        for (i <- 0 until memSpec.outputInstanceNum) {
          sinks += new MemSink(memSpec.internalArray,
                               memSpec.currentInstanceId, i)
        }
      }
    }
    outputSpec.outputMethod match {
      case OutputMethod.SINGLE => {
        assert(sinks.length == 1)
        sinks(0)
      }
      case OutputMethod.HASH => {
        new HashedSink(sinks.toArray)
      }
      case OutputMethod.HASH_AND_SORT => {
        new HashedSink(sinks.toArray.map(new SortedSink(_)))
      }
    }
  }
}