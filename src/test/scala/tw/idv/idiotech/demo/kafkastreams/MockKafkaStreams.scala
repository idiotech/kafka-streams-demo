package tw.idv.idiotech.demo.kafkastreams

import java.util.{Properties, UUID}

import org.apache.kafka.streams.{TestInputTopic, TestOutputTopic, Topology, TopologyTestDriver}
import tw.idv.idiotech.demo.kafkastreams.avro.{KeySerde, MockSchemaRegistry, ValueSerde}

trait MockKafkaStreams extends MockSchemaRegistry {

  def createTestDriver(topology: Topology): TopologyTestDriver = {
    val properties = new Properties()
    properties.setProperty("application.id", UUID.randomUUID().toString)
    properties.setProperty("bootstrap.servers", "dummy")
    new TopologyTestDriver(topology, properties)
  }

  def getInputTopic[K, V](topic: String)(implicit testDriver: TopologyTestDriver, keySerde: KeySerde[K], valueSerde: ValueSerde[V]): TestInputTopic[K, V] =
    testDriver.createInputTopic(topic, keySerde.serializer(), valueSerde.serializer())

  def getOutputTopic[K, V](topic: String)(implicit testDriver: TopologyTestDriver, keySerde: KeySerde[K], valueSerde: ValueSerde[V]): TestOutputTopic[K, V] =
    testDriver.createOutputTopic(topic, keySerde.deserializer(), valueSerde.deserializer())

  def pipeInput[K, V](inputTopic: TestInputTopic[K, V], k: K, v: V, timestamp: Long = System.currentTimeMillis())(
    testDriver: TopologyTestDriver
  ) = inputTopic.pipeInput(k, v, timestamp)

  def readOutput[K, V](outputTopic: TestOutputTopic[K, V])(
    implicit testDriver: TopologyTestDriver) =
    outputTopic.readKeyValue()

}
