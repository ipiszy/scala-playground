package ipiszy.scala.mapreduce

import java.io.File

import ipiszy.scala.mapreduce.MapReduceInputSpec
import org.scalatest.FunSuite

/**
  * Created by carolzhang on 12/1/15.
  */
class IOAdapterSuite extends FunSuite {
  IOAdapter.MIN_UNIT = 1
  val dataDir = "./src/test/ipiszy/scala/mapreduce/data/"
  val input = new MapReduceInputSpec(dataDir + "*.txt", FileFormat.TEXT)
  val textFileAdapter = IOAdapter.newIOAdapter(IOKind.FILE, FileFormat.TEXT)

  test("Split file to 1 unit.") {
    val result = textFileAdapter.splitInput(input, 1)
    assert(result.length === 1)
    assert(result(0).getKind === IOKind.FILE)
    assert(result(0).fileInput.fileChunks === Array[FileChunk](
      new FileChunk(new File(dataDir + "1.txt").getAbsolutePath, 0, 1),
      new FileChunk(new File(dataDir + "2.txt").getAbsolutePath, 0, 3),
      new FileChunk(new File(dataDir + "4.txt").getAbsolutePath, 0, 7)
    ))
  }

  test("Split file to 2 units.") {
    val result = textFileAdapter.splitInput(input, 2)
    assert(result.length === 2)
    assert(result(0).getKind === IOKind.FILE)
    assert(result(0).fileInput.fileChunks === Array[FileChunk](
      new FileChunk(new File(dataDir + "1.txt").getAbsolutePath, 0, 1),
      new FileChunk(new File(dataDir + "2.txt").getAbsolutePath, 0, 3),
      new FileChunk(new File(dataDir + "4.txt").getAbsolutePath, 0, 2)
    ))
    assert(result(1).getKind === IOKind.FILE)
    assert(result(1).fileInput.fileChunks === Array[FileChunk](
      new FileChunk(new File(dataDir + "4.txt").getAbsolutePath, 2, 7)
    ))
  }

  test ("Split file to n units, where n is greater than the total length.") {
    val result = textFileAdapter.splitInput(input, 100)
    assert(result.length === 100)
    var last = 0
    for (i <- 0 until 11) {
      assert(result(i).getKind === IOKind.FILE)
      assert(result(i).fileInput.fileChunks.length === 1)
      assert(result(i).fileInput.fileChunks(0).endOffset ===
        result(i).fileInput.fileChunks(0).beginOffset + 1)
      if (result(i).fileInput.fileChunks(0).beginOffset != 0) {
        assert(result(i).fileInput.fileChunks(0).beginOffset === last)
      }
      last = result(i).fileInput.fileChunks(0).endOffset
    }
    for (i <- 11 until 100) assert(result(i) === null)
  }
}
