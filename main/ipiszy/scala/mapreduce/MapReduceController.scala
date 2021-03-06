package ipiszy.scala.mapreduce

import java.io.File
import java.nio.file.{FileAlreadyExistsException, Files, Paths}
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
        new OutputSpec(new MemOutputSpec(mrInternalData, i, spec.numReducer),
                       OutputMethod.HASH_AND_SORT),
        spec.mapperClass, i)
    }
    result
  }

  private def genReducerSpec(spec: MapReduceSpec): Array[RunnerSpec] = {
    val result = new Array[RunnerSpec](spec.numReducer)
    for (i <- 0 until spec.numReducer) {
      result(i) = new RunnerSpec(
        new InputSpec(new MemInputSpec(mrInternalData, i, spec.numMapper),
                      InputMethod.SORT),
        new OutputSpec(
          Array(new FileOutputSpec(spec.output.filePrefix, i, spec.numReducer,
                                   spec.output.format)),
          OutputMethod.SINGLE),
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
    try {
      for (mapperSpec <- mapperSpecs) {
        mappers.execute(new MapperRunner(mapperSpec))
      }
    } finally {
      mappers.shutdown()
      while (!mappers.awaitTermination(1, TimeUnit.MINUTES)) {
        println("Waiting for mappers to complete.")
      }
    }
    val outputDir = Paths.get(new File(spec.output.filePrefix).getParent)
    println("Creating output directory: " + outputDir + ".")
    try {
      Files.createDirectory(outputDir)
      println("The output directory was created successfully.")
    } catch {
      case _: FileAlreadyExistsException => {
        println("Output directory already exists.")
      }
    }
    println("Start reducers.")
    val reducers = Executors.newFixedThreadPool(spec.numReducer)
    try {
      for (reducerSpec <- reducerSpecs) {
        reducers.execute(new ReducerRunner(reducerSpec))
      }
    } finally {
      reducers.shutdown()
      while (!reducers.awaitTermination(1, TimeUnit.MINUTES)) {
        println("Waiting for reducers to complete.")
      }
    }
    println("Completed.")
  }
}
