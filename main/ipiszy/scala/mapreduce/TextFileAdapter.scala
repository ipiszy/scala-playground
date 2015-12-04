package ipiszy.scala.mapreduce

import java.io.RandomAccessFile

/**
  * Created by carolzhang on 11/30/15.
  */
class TextFileSource(val fileInputSpec: FileInputSpec) extends Source {
  private var idx = 0
  private var file: RandomAccessFile = null

  override def readNextValue(): Array[Byte] = {
    if (fileInputSpec == null || idx >= fileInputSpec.fileChunks.length)
      return null
    if (file == null) {
      file = new RandomAccessFile(fileInputSpec.fileChunks(idx).fileName, "r")
      if (fileInputSpec.fileChunks(idx).beginOffset > 0) {
        // Read until the first \n.
        file.seek(fileInputSpec.fileChunks(idx).beginOffset - 1)
        file.readLine()
      }
    }
    if (file.getFilePointer >= fileInputSpec.fileChunks(idx).endOffset) {
      file.close()
      file = null
      idx += 1
      readNextValue()
    } else {
      file.readLine().getBytes()
    }
  }

  override def readNextKV(): (Array[Byte], Array[Byte]) = {
    assert(false, "This method hasn't been implemented yet!")
    (null, null)
  }
}

class TextFileSink(val fileOutputSpec: FileOutputSpec) extends Sink {
  val fileName = fileOutputSpec.filePrefix + "-" +
    fileOutputSpec.currentInstanceId + "-of-" + fileOutputSpec.outputInstanceNum
  val file = new RandomAccessFile(fileName, "w")

  override def writeKV(key: Array[Byte], value: Array[Byte]): Unit = {
    file.write(key)
    file.writeChar(':')
    file.write(value)
    file.writeChar('\n')
  }

  override def close(): Unit = {
    file.close()
  }
}

class TextFileAdapter extends IOAdapter {
  override def source(spec: InputSpec): Source = {
    assert(spec.getKind == IOKind.FILE)
    assert(spec.fileInput.fileFormat == FileFormat.TEXT)
    new TextFileSource(spec.fileInput)
  }

  override def sink(spec: OutputSpec): Sink = {
    assert(spec.getKind == IOKind.FILE)
    assert(spec.fileOutput.fileFormat == FileFormat.TEXT)
    new TextFileSink(spec.fileOutput)
  }
}
