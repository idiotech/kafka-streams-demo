package tw.idv.idiotech.demo.kafkastreams.avro

import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Joined, Materialized, Produced}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer, Serdes => JSerdes}

object StreamsImplicits {

  trait KeyValueSerde[T] extends KeySerde[T] with ValueSerde[T]
  implicit def numberSerde[V <: AnyVal](implicit serde: Serde[V]) = new KeyValueSerde[V] {
    override def serializer(): Serializer[V] = serde.serializer()
    override def deserializer(): Deserializer[V] = serde.deserializer()
  }

  implicit val stringSerde = new KeyValueSerde[String] {
    override def serializer(): Serializer[String] = JSerdes.String().serializer()
    override def deserializer(): Deserializer[String] = JSerdes.String().deserializer()
  }
  implicit def String: Serde[String] = JSerdes.String()
  implicit def Long: Serde[Long] = JSerdes.Long().asInstanceOf[Serde[Long]]
  implicit def JavaLong: Serde[java.lang.Long] = JSerdes.Long()
  implicit def ByteArray: Serde[Array[Byte]] = JSerdes.ByteArray()
  implicit def Bytes: Serde[org.apache.kafka.common.utils.Bytes] = JSerdes.Bytes()
  implicit def Float: Serde[Float] = JSerdes.Float().asInstanceOf[Serde[Float]]
  implicit def JavaFloat: Serde[java.lang.Float] = JSerdes.Float()
  implicit def Double: Serde[Double] = JSerdes.Double().asInstanceOf[Serde[Double]]
  implicit def JavaDouble: Serde[java.lang.Double] = JSerdes.Double()
  implicit def Integer: Serde[Int] = JSerdes.Integer().asInstanceOf[Serde[Int]]
  implicit def JavaInteger: Serde[java.lang.Integer] = JSerdes.Integer()

  implicit def groupedFromSerde[K, V](implicit keySerde: KeySerde[K], valueSerde: ValueSerde[V]): Grouped[K, V] =
    Grouped.`with`[K, V]

  implicit def consumedFromSerde[K, V](implicit keySerde: KeySerde[K], valueSerde: ValueSerde[V]): Consumed[K, V] =
    Consumed.`with`[K, V]

  implicit def producedFromSerde[K, V](implicit keySerde: KeySerde[K], valueSerde: ValueSerde[V]): Produced[K, V] =
    Produced.`with`[K, V]

  implicit def materializedFromSerde[K, V, S <: StateStore](implicit keySerde: KeySerde[K],
                                                            valueSerde: ValueSerde[V]): Materialized[K, V, S] =
    Materialized.`with`[K, V, S]

  implicit def joinedFromKeyValueOtherSerde[K, V, VO](implicit keySerde: KeySerde[K],
                                                      valueSerde: ValueSerde[V],
                                                      otherValueSerde: ValueSerde[VO]): Joined[K, V, VO] =
    Joined.`with`[K, V, VO]
}
