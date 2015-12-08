package ipiszy.scala.mapreduce

/**
  * Created by carolzhang on 11/30/15.
  */
class MapperRunner(val mapperSpec: RunnerSpec) extends Runnable {
  val input = Source.newSource(mapperSpec.input)
  val output = Sink.newSink(mapperSpec.output)
  val mapper: Mapper = Class.forName(mapperSpec.className)
    .asInstanceOf[Class[Mapper]].newInstance()

  override def run(): Unit = {
    println("Starting mapper #" + mapperSpec.instanceId + ".")
    try {
      var inputValue = input.readNextValue()
      while (inputValue != null) {
        val mapResults = mapper.map(inputValue)
        if (mapResults != null) mapResults.foreach(output.writeKV)
        inputValue = input.readNextValue()
      }
    } finally {
      output.close()
    }
    println("Mapper #" + mapperSpec.instanceId + " has completed!")
  }
}
