package tw.idv.idiotech.demo.kafkastreams

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import tw.idv.idiotech.kafkastreams.avro.StreamsImplicits

trait StreamsHandsOn extends AvroSerdes with StreamsImplicits {

  type InfluencerId = String
  def topology(): Topology = {

    val builder = new StreamsBuilder()
    val followTable = builder.table[FollowPair, FollowStatus]("follows")
    val followerCountTable: KTable[InfluencerId, Long] = followTable
      .filter((_, v) => !v.deleted)
      .groupBy((k, v) => (k.influencerId, v)).count()
    val followerCountStream = followerCountTable.toStream
    followerCountStream.to("follower-counts")
    builder.build()
  }

}
