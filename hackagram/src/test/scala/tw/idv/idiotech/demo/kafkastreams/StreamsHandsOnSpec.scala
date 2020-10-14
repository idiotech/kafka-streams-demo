package tw.idv.idiotech.demo.kafkastreams

import java.time.{ ZoneId, ZonedDateTime }

import org.apache.kafka.streams.TopologyTestDriver
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.JavaConverters._

class StreamsHandsOnSpec extends AnyFlatSpec with Matchers with MockKafkaStreams with StreamsHandsOn {

  val stella = User("stella", "Stella Hackworth")
  val colin = User("colin", "Colin Codesmith")
  val stalker = User("stalker", "Stalker")

  val thirteen = ZonedDateTime.of(2020, 10, 10, 13, 0, 0, 0, ZoneId.of("Asia/Taipei"))
  val fourteen = thirteen.plusHours(1)
  val fifteen = thirteen.plusHours(2)

  val blueSunset =
    Photo("photo-1", "http://blue.sunset/image", stella.id, fifteen.toInstant.toEpochMilli)

  val blueFlower =
    Photo("photo-2", "http://blue.flower/image", stella.id, thirteen.toInstant.toEpochMilli)

  val lonelyFigure =
    Photo("photo-3", "http://lonely.figure/image", colin.id, fourteen.toInstant.toEpochMilli)

  val hackagramTopology = topology()

  println(hackagramTopology.describe())

  def produceUsers()(implicit testDriver: TopologyTestDriver) = {
    val userInput = getInputTopic[String, User]("users")
    userInput.pipeInput(stella.id, stella)
    userInput.pipeInput(colin.id, colin)
    userInput.pipeInput(stalker.id, stalker)
  }

  def producePhotos()(implicit testDriver: TopologyTestDriver) = {
    val photoInput = getInputTopic[String, Photo]("photos")
    photoInput.pipeInput(blueSunset.id, blueSunset)
    photoInput.pipeInput(blueFlower.id, blueFlower)
    photoInput.pipeInput(lonelyFigure.id, lonelyFigure)
  }

  def produceFollowers()(implicit testDriver: TopologyTestDriver) = {
    val followInput = getInputTopic[FollowPair, FollowStatus]("follows")
    followInput.pipeInput(
      FollowPair(stella.id, stalker.id),
      FollowStatus(System.currentTimeMillis())
    )
    followInput.pipeInput(
      FollowPair(colin.id, stalker.id),
      FollowStatus(System.currentTimeMillis())
    )
  }

  "streams" must "calculate follower counts" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceFollowers()
    val followInput = getInputTopic[FollowPair, FollowStatus]("follows")
    followInput.pipeInput(
      FollowPair(stella.id, colin.id),
      FollowStatus(System.currentTimeMillis())
    )
    val followerCountTopic = getOutputTopic[String, Long]("follower-counts")
    followerCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "stella" -> 2,
      "colin" -> 1
    )


  }
}
