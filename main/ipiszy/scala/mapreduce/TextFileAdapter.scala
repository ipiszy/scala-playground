package ipiszy.scala.mapreduce

import java.io.{PrintWriter, FileWriter, RandomAccessFile}
import java.nio.file.{Paths, Files}

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
  Files.deleteIfExists(Paths.get(fileName))
  val file = new PrintWriter(fileName)

  override def writeKV(kv: (Array[Byte], Array[Byte])): Unit = {
    file.println(new String(kv._1) + " : " + new String(kv._2))
  }

  override def close(): Unit = {
    file.close()
  }
}

class TextFileAdapter extends IOAdapter {}
