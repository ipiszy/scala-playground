package ipiszy.scala.mapreduce

import java.util.concurrent.{TimeUnit, Executors}

/**
  * Created by carolzhang on 11/30/15.
  */
class MapReduceController(val spec: MapReduceSpec) {
  private val mrInternalData: Array[Array[Array[Byte]]] =
    initMRInternalData(spec)
  private val mapperSpecs: Array[RunnerSpec] = genMapperSpec(spec)
  private val reducerSpecs: Array[RunnerSpec] = genReducerSpec(spec)

  private def genMapperSpec(spec: MapReduceSpec): Array[RunnerSpec] = {
    val result = new Array[RunnerSpec](spec.numMapper)
    val inputSpecs = IOAdapter.newIOAdapter(IOKind.FILE, spec.input.format)
      .splitInput(spec.input, spec.numMapper)
    for (i <- 0 until spec.numMapper) {
      result(i) = new RunnerSpec(
        inputSpecs(i),
        new OutputSpec(new MemOutputSpec(
          mrInternalData, i, spec.numReducer, PartitionMethod.SHUFFLE)),
          spec.mapperClass, i)
    }
    result
  }

  private def genReducerSpec(spec: MapReduceSpec): Array[RunnerSpec] = {
    val result = new Array[RunnerSpec](spec.numReducer)
    val inputSpecs = IOAdapter.newIOAdapter(IOKind.FILE, spec.input.format)
      .splitInput(spec.input, spec.numMapper)
    for (i <- 0 until spec.numMapper) {
      result(i) = new RunnerSpec(
        new InputSpec(new MemInputSpec(mrInternalData, i, spec.numMapper)),
        new OutputSpec(new FileOutputSpec(
          spec.output.filePrefix, i, spec.numReducer, spec.output.format)),
        spec.reducerClass, i)
    }
    result
  }

  private def initMRInternalData(spec: MapReduceSpec)
      : Array[Array[Array[Byte]]] = {
    Array.ofDim[Array[Byte]](spec.numReducer, spec.numMapper)
  }

  def start(): Unit = {
    println("Start mappers.")
    val mappers = Executors.newFixedThreadPool(spec.numMapper)
    for (mapperSpec <- mapperSpecs) {
      mappers.execute(new MapperRunner(mapperSpec))
    }
    mappers.shutdown()
    while (!mappers.awaitTermination(1, TimeUnit.MINUTES)) {
      println("Waiting for mappers to complete.")
    }
    println("Start reducers.")
    val reducers = Executors.newFixedThreadPool(spec.numReducer)
    for (reducerSpec <- reducerSpecs) {
      reducers.execute(new ReducerRunner(reducerSpec))
    }
    reducers.shutdown()
    while (!reducers.awaitTermination(1, TimeUnit.MINUTES)) {
      println("Waiting for reducers to complete.")
    }
    println("Completed.")
  }
}
