package ipiszy.scala.mapreduce

import scala.collection.mutable.ArrayBuffer

/**
  * Created by carolzhang on 11/30/15.
  */
trait Source {
  def readNextValue(): Array[Byte]
  def readNextKV(): (Array[Byte], Array[Byte])
}

object Source {
  def newFileSource(fileInputSpec: FileInputSpec) : Source = {
    fileInputSpec.fileFormat match {
      case FileFormat.TEXT => new TextFileSource(fileInputSpec.fileChunks)
    }
  }

  def newSource(inputSpec: InputSpec): Source = {
    val sources = new ArrayBuffer[Source]()
    inputSpec.getKind match {
      case IOKind.FILE => {
        inputSpec.fileInput.foreach(sources += newFileSource(_))
      }
      case IOKind.MEMORY => {
        val memSpec = inputSpec.memInputSpec
        for (i <- 0 until memSpec.inputInstanceNum) {
          sources += new MemSource(memSpec.internalArray,
                                   memSpec.currentInstanceId, i)
        }
      }
    }
    inputSpec.inputMethod match {
      case InputMethod.SINGLE => {
        assert(sources.length == 1)
        sources(0)
      }
      case InputMethod.SORT => {
        new SortedSource(sources.toArray)
      }
    }
  }
}
