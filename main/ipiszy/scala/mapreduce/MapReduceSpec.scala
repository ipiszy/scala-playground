package ipiszy.scala.mapreduce

/**
  * Created by carolzhang on 11/30/15.
  */

// Public usage.
object FileFormat extends Enumeration {
  val TEXT = Value
}
class MapReduceInputSpec(val filePattern: String,
                         val format: FileFormat.Value) {}
class MapReduceOutputSpec(val filePrefix: String,
                          val format: FileFormat.Value) {}
class MapReduceSpec(val input: MapReduceInputSpec,
                    val output: MapReduceOutputSpec,
                    val mapperClass: String,
                    val numMapper: Int,
                    val reducerClass: String,
                    val numReducer: Int) {}

// Internal usage only.
object PartitionMethod extends Enumeration {
  val SHUFFLE = Value
}
object IOKind extends Enumeration {
  val FILE = Value
  val MEMORY = Value
}
class FileChunk(val fileName: String, val beginOffset: Int,
                val endOffset: Int) {
  def this() = {this("", 0, 0)}
  override def toString() = fileName + ":" + beginOffset.toString + ":" +
    endOffset.toString
  override def equals(other: Any) = {
    val that = other.asInstanceOf[FileChunk]
    if (that == null) false
    else fileName.equals(that.fileName) && beginOffset == that.beginOffset &&
      endOffset == that.endOffset
  }
}
class FileInputSpec(val fileChunks: Array[FileChunk],
                    val fileFormat: FileFormat.Value) {
  def this() = {this(null, FileFormat.TEXT)}
  override def toString() = "FileChunks: [" + fileChunks.mkString(", ") + "]," +
    ", FileFormat: " + fileFormat.toString

  def canEqual(other: Any): Boolean = other.isInstanceOf[FileInputSpec]

  override def equals(other: Any): Boolean = other match {
    case that: FileInputSpec =>
      (that canEqual this) &&
        fileChunks == that.fileChunks &&
        fileFormat == that.fileFormat
    case _ => false
  }
}

class MemInputSpec(val internalArray: Array[Array[Array[Byte]]],
                   val currentInstanceId: Int,
                   val inputInstanceNum: Int) {
  def this() = {this(null, 0, 0)}

  def canEqual(other: Any): Boolean = other.isInstanceOf[MemInputSpec]

  override def equals(other: Any): Boolean = other match {
    case that: MemInputSpec =>
      (that canEqual this) &&
        internalArray == that.internalArray &&
        currentInstanceId == that.currentInstanceId &&
        inputInstanceNum == that.inputInstanceNum
    case _ => false
  }
}

class FileOutputSpec(val filePrefix: String,
                     val currentInstanceId: Int,
                     val outputInstanceNum: Int,
                     val fileFormat: FileFormat.Value) {
  def this() = {this("", 0, 0, FileFormat.TEXT)}
}
class MemOutputSpec(val internalArray: Array[Array[Array[Byte]]],
                    val currentInstanceId: Int,
                    val outputInstanceNum: Int,
                    val partitionMethod: PartitionMethod.Value) {
  def this() = {this(null, 0, 0, PartitionMethod.SHUFFLE)}
}

class InputSpec(val memInputSpec: MemInputSpec,
                val fileInput: FileInputSpec) {
  private var source: IOKind.Value = IOKind.FILE
  def this(memInputSpec: MemInputSpec) = {
    this(memInputSpec, new FileInputSpec())
    source = IOKind.MEMORY
  }
  def this(fileInputSpec: FileInputSpec) = {
    this(new MemInputSpec(), fileInputSpec)
    source = IOKind.FILE
  }
  override def toString() = "MemInputSpec: " + memInputSpec.toString + "\n" +
    "FileInputSpec: " + fileInput

  def getKind = source

  override def equals(other: Any) = {
    val that = other.asInstanceOf[InputSpec]
    if (that == null) false
    else source.equals(that.source) && memInputSpec.equals(that.memInputSpec) &&
      fileInput.equals(that.fileInput)
  }
}

class OutputSpec(val memOutputSpec: MemOutputSpec,
                 val fileOutput: FileOutputSpec) {
  private var source: IOKind.Value = IOKind.FILE
  def this(memOutputSpec: MemOutputSpec) = {
    this(memOutputSpec, new FileOutputSpec())
    source = IOKind.MEMORY
  }
  def this(fileOutput: FileOutputSpec) = {
    this(new MemOutputSpec(), fileOutput)
    source = IOKind.FILE
  }
  def getKind = source
}

class RunnerSpec(val input: InputSpec, val output: OutputSpec,
                 val className: String, val instanceId: Int) {}