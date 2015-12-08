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
  override def toString = fileName + ":" + beginOffset.toString + ":" +
    endOffset.toString
  override def equals(other: Any) = {
    val that = other.asInstanceOf[FileChunk]
    if (that == null) false
    else fileName == that.fileName && beginOffset == that.beginOffset &&
      endOffset == that.endOffset
  }
}
class FileInputSpec(val fileChunks: Array[FileChunk],
                    val fileFormat: FileFormat.Value) {
  override def toString = "FileChunks: [" + fileChunks.mkString(", ") + "]," +
    ", FileFormat: " + fileFormat.toString

  def canEqual(other: Any): Boolean = other.isInstanceOf[FileInputSpec]

  override def equals(other: Any): Boolean = other match {
    case that: FileInputSpec =>
      (that canEqual this) &&
        fileChunks.sameElements(that.fileChunks) &&
        fileFormat == that.fileFormat
    case _ => false
  }
}

class MemInputSpec(val internalArray: Array[Array[Array[Byte]]],
                   val currentInstanceId: Int,
                   val inputInstanceNum: Int) {

  def canEqual(other: Any): Boolean = other.isInstanceOf[MemInputSpec]

  override def equals(other: Any): Boolean = other match {
    case that: MemInputSpec =>
      (that canEqual this) &&
        (internalArray sameElements that.internalArray) &&
        currentInstanceId == that.currentInstanceId &&
        inputInstanceNum == that.inputInstanceNum
    case _ => false
  }
}

class FileOutputSpec(val filePrefix: String,
                     val currentInstanceId: Int,
                     val outputInstanceNum: Int,
                     val fileFormat: FileFormat.Value) {}
class MemOutputSpec(val internalArray: Array[Array[Array[Byte]]],
                    val currentInstanceId: Int,
                    val outputInstanceNum: Int) {}

object InputMethod extends Enumeration {
  val SINGLE = Value
  val SORT = Value
}
class InputSpec private(val memInputSpec: MemInputSpec,
                        val fileInput: Array[FileInputSpec],
                        val inputMethod: InputMethod.Value,
                        private val source: IOKind.Value) {
  def this(memInputSpec: MemInputSpec,
           inputMethod: InputMethod.Value) = {
    this(memInputSpec, null, inputMethod, IOKind.MEMORY)
  }
  def this(fileInputSpec: Array[FileInputSpec],
           inputMethod: InputMethod.Value) = {
    this(null, fileInputSpec, inputMethod, IOKind.FILE)
  }
  override def toString = "MemInputSpec: " + memInputSpec.toString + "\n" +
    "FileInputSpec: " + fileInput

  def getKind = source

  override def equals(other: Any) = {
    val that = other.asInstanceOf[InputSpec]
    if (that == null) false
    else source.equals(that.source) && memInputSpec.equals(that.memInputSpec) &&
      fileInput.sameElements(that.fileInput)
  }
}

object OutputMethod extends Enumeration {
  val SINGLE = Value
  val HASH = Value
  val HASH_AND_SORT = Value
}
class OutputSpec private(val memOutputSpec: MemOutputSpec,
                         val fileOutput: Array[FileOutputSpec],
                         val outputMethod: OutputMethod.Value) {
  private var source: IOKind.Value = IOKind.FILE
  def this(memOutputSpec: MemOutputSpec,
           outputMethod: OutputMethod.Value) = {
    this(memOutputSpec, null, outputMethod)
    source = IOKind.MEMORY
  }
  def this(fileOutput: Array[FileOutputSpec],
           outputMethod: OutputMethod.Value) = {
    this(null, fileOutput, outputMethod)
    source = IOKind.FILE
  }
  def getKind = source
}

class RunnerSpec(val input: InputSpec, val output: OutputSpec,
                 val className: String, val instanceId: Int) {}