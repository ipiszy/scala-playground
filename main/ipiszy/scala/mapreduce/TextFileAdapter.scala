package ipiszy.scala.mapreduce

import java.io.RandomAccessFile

/**
  * Created by carolzhang on 11/30/15.
  */
class TextFileSource(val fileChunks: Array[FileChunk]) extends Source {
  private var idx = 0
  private var file: RandomAccessFile = null

  override def readNextValue(): Array[Byte] = {
    if (idx >= fileChunks.length)
      return null
    if (file == null) {
      file = new RandomAccessFile(fileChunks(idx).fileName, "r")
      if (fileChunks(idx).beginOffset > 0) {
        // Read until the first \n.
        file.seek(fileChunks(idx).beginOffset - 1)
        file.readLine()
      }
    }
    if (file.getFilePointer >= fileChunks(idx).endOffset) {
      file.close()
      file = null
      idx += 1
      readNextValue()
    } else {
      file.readLine().getBytes()
    }
  }
}

class TextFileSink(val filePrefix: String,
                   val currentInstanceId: Int,
                   val outputInstanceNum: Int) extends Sink {
  val fileName = filePrefix + "-" + currentInstanceId + "-of-" +
    outputInstanceNum
  val file = new RandomAccessFile(fileName, "rw")

  override def writeKV(kv: (Array[Byte], Array[Byte])): Unit = {
    file.write(kv._1)
    file.writeChar(':')
    file.write(kv._2)
    file.writeChar('\n')
  }

  override def close(): Unit = {
    file.close()
  }
}

class TextFileAdapter extends IOAdapter {}
