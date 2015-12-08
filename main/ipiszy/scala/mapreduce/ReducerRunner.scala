package ipiszy.scala.mapreduce

/**
  * Created by carolzhang on 11/30/15.
  */
class ReducerRunner(val reducerSpec: RunnerSpec) extends Runnable {
  val input = Source.newSource(reducerSpec.input)
  val output = Sink.newSink(reducerSpec.output)
  val reducer: Reducer = Class.forName(reducerSpec.className)
    .asInstanceOf[Class[Reducer]].newInstance()

  override def run(): Unit = {
    println("Starting reducer #" + reducerSpec.instanceId + ".")
    try {
      var inputKVs = input.readNextKVs()
      while (inputKVs != null) {
        val result = reducer.reduce(inputKVs._1, inputKVs._2)
        if (result != null) output.writeKV(result)
        inputKVs = input.readNextKVs()
      }
    } finally {
      output.close()
    }
    println("Reducer #" + reducerSpec.instanceId + " has completed!")
  }

}
