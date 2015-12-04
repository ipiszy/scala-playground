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
    var inputKV = input.readNextKV()
    while (inputKV != null) {
      output.writeKV(reducer.reduce(inputKV._1, inputKV._2))
      inputKV = input.readNextKV()
    }
    println("Reducer #" + reducerSpec.instanceId + " has completed!")
  }

}
