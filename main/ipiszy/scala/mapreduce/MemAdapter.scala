package ipiszy.scala.mapreduce

import java.io._

/**
  * Created by carolzhang on 11/30/15.
  */
class MemSource(val memInputSpec: MemInputSpec) extends Source {
  private var idx = 0
  private var objectIS: ObjectInputStream = null

  override def readNextValue(): Array[Byte] = {
    assert(false, "Unimplemented method!")
    null
  }

  override def readNextKV(): (Array[Byte], Array[Byte]) = {
    if (idx >= memInputSpec.inputInstanceNum) return null
    if (objectIS == null) {
      objectIS = new ObjectInputStream(new ByteArrayInputStream(
        memInputSpec.internalArray(memInputSpec.currentInstanceId)(idx)))
    }
    try {
      val key = objectIS.readObject().asInstanceOf[Array[Byte]]
      val value = objectIS.readObject().asInstanceOf[Array[Byte]]
      (key, value)
    } catch {
      case _: EOFException => {
        objectIS.close()
        idx += 1
        readNextKV()
      }
    }
  }

}

class MemSink(val memOutputSpec: MemOutputSpec) extends Sink {
  private val byteArrayOS = new Array[ByteArrayOutputStream](
    memOutputSpec.outputInstanceNum)
  private val objectOS = new Array[ObjectOutputStream](
    memOutputSpec.outputInstanceNum)
  for (i <- 0 until objectOS.length) {
    byteArrayOS(i) = new ByteArrayOutputStream()
    objectOS(i) = new ObjectOutputStream(byteArrayOS(i))
  }

  override def writeKV(key: Array[Byte], value: Array[Byte]): Unit = {
    val idx = key.hashCode() % memOutputSpec.outputInstanceNum
    objectOS(idx).writeObject(key)
    objectOS(idx).writeObject(value)
  }

  override def close(): Unit = {
    for (i <- 0 until objectOS.length) {
      objectOS(i).flush()
      memOutputSpec.internalArray(memOutputSpec.currentInstanceId)(i) =
        byteArrayOS(i).toByteArray
      objectOS(i).close()
    }
  }

}

class MemAdapter extends IOAdapter {
  override def source(spec: InputSpec): Source = {
    assert(spec.getKind == IOKind.MEMORY)
    new MemSource(spec.memInputSpec)
  }

  override def sink(spec: OutputSpec): Sink = {
    assert(spec.getKind == IOKind.MEMORY)
    new MemSink(spec.memOutputSpec)
  }
}
