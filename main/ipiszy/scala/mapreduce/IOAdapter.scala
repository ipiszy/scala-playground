package ipiszy.scala.mapreduce

import java.io.File
import java.nio.file.{Paths, Files}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by carolzhang on 11/30/15.
  */
trait IOAdapter {
  def splitInput(mrInputSpec: MapReduceInputSpec,
                 numInstance: Int): Array[InputSpec] = {
    // Get file patterns.
    val filePattern = new File(mrInputSpec.filePattern)
    val dir = filePattern.getParent
    val name = filePattern.getName
    var it = Files.newDirectoryStream(Paths.get(dir), name).iterator()
    // Get the total length of files.
    var length = 0
    while (it.hasNext) {
      val file = it.next().toFile
      require(file.isFile,
        "The pattern does not match a file: " + file.getAbsolutePath)
      length += file.length().toInt
    }
    // Get the length for each map shard.
    val lengthUnit =
      if (length / IOAdapter.MIN_UNIT < numInstance) IOAdapter.MIN_UNIT
      else length / numInstance + length % numInstance
    // Split files into file chunks.
    val result = new Array[InputSpec](numInstance)
    it = Files.newDirectoryStream(Paths.get(dir)).iterator()
    var beginOffset = 0
    var remainingOffset = lengthUnit
    var fileChunks = new ArrayBuffer[FileChunk]()
    var i = 0
    while (it.hasNext) {
      val file = it.next().toFile
      while (file.length() - beginOffset >= remainingOffset) {
        fileChunks += new FileChunk(file.getAbsolutePath, beginOffset,
          beginOffset + remainingOffset)
        result(i) = new InputSpec(new FileInputSpec(fileChunks.toArray,
          mrInputSpec.format))
        i += 1
        fileChunks = new ArrayBuffer[FileChunk]()
        beginOffset += remainingOffset
        remainingOffset = lengthUnit
      }
      if (file.length() > beginOffset) {
        fileChunks += new FileChunk(file.getAbsolutePath, beginOffset,
          file.length().toInt)
        remainingOffset -= file.length().toInt - beginOffset
      }
      beginOffset = 0
    }
    if (!fileChunks.isEmpty) result(i) = new InputSpec(
      new FileInputSpec(fileChunks.toArray, mrInputSpec.format))
    result
  }

  def source(spec: InputSpec): Source
  def sink(spec: OutputSpec): Sink
}

object IOAdapter {
  def newIOAdapter(kind: IOKind.Value, format: FileFormat.Value): IOAdapter = {
    kind match {
      case IOKind.MEMORY => new MemAdapter()
      case IOKind.FILE => {
        format match {
          case FileFormat.TEXT => new TextFileAdapter()
          case _ => throw new IllegalArgumentException("Unsupported file " +
            "format.")
        }
      }
      case _ => throw new IllegalArgumentException("Unsupported IOKind.")
    }
  }
  var MIN_UNIT = 256
}