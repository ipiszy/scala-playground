package ipiszy.scala.mapreduce

import java.io._

/**
  * Created by carolzhang on 11/30/15.
  */
class MemSource(val internalArray: Array[Array[Array[Byte]]],
                val currentInstanceId: Int,
                val inputInstanceId: Int) extends Source {
  private val objectIS: ObjectInputStream =
    new ObjectInputStream(new ByteArrayInputStream(
      internalArray(currentInstanceId)(inputInstanceId)))

  override def readNextValue(): Array[Byte] = {
    assert(false, "Unimplemented method!")
    null
  }

  override def readNextKV(): (Array[Byte], Array[Byte]) = {
    try {
      val key = objectIS.readObject().asInstanceOf[Array[Byte]]
      val value = objectIS.readObject().asInstanceOf[Array[Byte]]
      (key, value)
    } catch {
      case _: EOFException => {
        objectIS.close()
        null
      }
    }
  }
}

class MemSink(val internalArray: Array[Array[Array[Byte]]],
              val currentInstanceId: Int,
              val outputInstanceId: Int) extends Sink {
  private val byteArrayOS = new ByteArrayOutputStream()
  private val objectOS = new ObjectOutputStream(byteArrayOS)

  override def writeKV(kv: (Array[Byte], Array[Byte])): Unit = {
    objectOS.writeObject(kv._1)
    objectOS.writeObject(kv._2)
  }

  override def close(): Unit = {
    objectOS.flush()
    internalArray(outputInstanceId)(currentInstanceId) = byteArrayOS.toByteArray
    objectOS.close()
  }

}

class MemAdapter extends IOAdapter {
  override def splitInput(mrInputSpec: MapReduceInputSpec,
                          numInstance: Int): Array[InputSpec] = {
    assert(false, "Unimplemented method!")
    null
  }
  /*
  override def source(spec: InputSpec): Source = {
    assert(spec.getKind == IOKind.MEMORY)
    new MemSource(spec.memInputSpec)
  }

  override def sink(spec: OutputSpec): Sink = {
    assert(spec.getKind == IOKind.MEMORY)
    new MemSink(spec.memOutputSpec)
  }
  */
}
