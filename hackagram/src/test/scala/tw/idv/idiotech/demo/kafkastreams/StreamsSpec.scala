package tw.idv.idiotech.demo.kafkastreams

import java.time.{ ZoneId, ZonedDateTime }

import org.apache.kafka.streams.TopologyTestDriver
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.collection.JavaConverters._
import tw.idv.idiotech.kafkastreams.avro.StreamsImplicits._

class StreamsSpec extends AnyFlatSpec with Matchers with MockKafkaStreams with Streams {

  val stella = User("stella", "Stella Hackworth")
  val colin = User("colin", "Colin Codesmith")
  val stalker = User("stalker", "Stalker")

  val thirteen = ZonedDateTime.of(2020, 10, 10, 13, 0, 0, 0, ZoneId.of("Asia/Taipei"))
  val fourteen = thirteen.plusHours(1)
  val fifteen = thirteen.plusHours(2)

  val blueSunset =
    Photo("photo-1", "http://blue.sunset/image", stella.id, fifteen.toInstant.toEpochMilli, false)

  val blueFlower =
    Photo("photo-2", "http://blue.flower/image", stella.id, thirteen.toInstant.toEpochMilli, false)

  val lonelyFigure =
    Photo("photo-3", "http://lonely.figure/image", colin.id, fourteen.toInstant.toEpochMilli, false)

  val hackagramTopology = topology()

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
      FollowStatus(System.currentTimeMillis(), false)
    )
    followInput.pipeInput(
      FollowPair(colin.id, stalker.id),
      FollowStatus(System.currentTimeMillis(), false)
    )
  }

  "streams" must "create timeline for user" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()
    producePhotos()

    val timelineOutput = getOutputTopic[String, Timeline]("timeline")
    val results = timelineOutput.readKeyValuesToMap().asScala
    results("stalker").data mustBe List(
      DenormalizedPhoto(blueSunset, stella),
      DenormalizedPhoto(lonelyFigure, colin),
      DenormalizedPhoto(blueFlower, stella)
    )
    results.size mustBe 1
  }

  it must "delete image from timeline" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()
    producePhotos()
    val timelineOutput = getOutputTopic[String, Timeline]("timeline")
    timelineOutput.readKeyValuesToMap()
    val photoInput = getInputTopic[String, Photo]("photos")
    photoInput.pipeInput(lonelyFigure.id, lonelyFigure.copy(deleted = true))
    val results = timelineOutput.readKeyValuesToMap().asScala
    results("stalker").data mustBe List(
      DenormalizedPhoto(blueSunset, stella),
      DenormalizedPhoto(blueFlower, stella)
    )
  }

  it must "delete images from a user who is unfollowed" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()
    producePhotos()
    val timelineOutput = getOutputTopic[String, Timeline]("timeline")
    timelineOutput.readKeyValuesToMap()
    val followInput = getInputTopic[FollowPair, FollowStatus]("follows")
    followInput.pipeInput(
      FollowPair(colin.id, stalker.id),
      FollowStatus(System.currentTimeMillis(), true)
    )
    val results = timelineOutput.readKeyValuesToMap().asScala
    results("stalker").data mustBe List(
      DenormalizedPhoto(blueSunset, stella),
      DenormalizedPhoto(blueFlower, stella)
    )
  }

  it must "add photos to timeline when the user follows another user" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    producePhotos()
    produceFollowers()
    val timelineOutput = getOutputTopic[String, Timeline]("timeline")
    val results = timelineOutput.readKeyValuesToMap().asScala
    results("stalker").data mustBe List(
      DenormalizedPhoto(blueSunset, stella),
      DenormalizedPhoto(lonelyFigure, colin),
      DenormalizedPhoto(blueFlower, stella)
    )
    results.size mustBe 1
  }

  it must "calculate follower counts" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()

    val followInput = getInputTopic[FollowPair, FollowStatus]("follows")
    followInput.pipeInput(
      FollowPair(stella.id, colin.id),
      FollowStatus(System.currentTimeMillis(), false)
    )

    val followerCountTopic = getOutputTopic[String, Long]("follower_count")
    followerCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "stella" -> 2,
      "colin"  -> 1
    )

    val influencerCountTopic = getOutputTopic[String, Long]("influencer_count")
    influencerCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "colin"   -> 1,
      "stalker" -> 2
    )
  }

  it must "correctly recompute followers count in case of an unfollow" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()

    val followInput = getInputTopic[FollowPair, FollowStatus]("follows")
    followInput.pipeInput(
      FollowPair(stella.id, colin.id),
      FollowStatus(System.currentTimeMillis(), false)
    )
    followInput.pipeInput(
      FollowPair(stella.id, colin.id),
      FollowStatus(System.currentTimeMillis(), true)
    )

    val followerCountTopic = getOutputTopic[String, Long]("follower_count")
    followerCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "stella" -> 1,
      "colin"  -> 1
    )

    val influencerCountTopic = getOutputTopic[String, Long]("influencer_count")
    influencerCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "colin"   -> 0,
      "stalker" -> 2
    )
  }

  it must "calculate photo counts" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    producePhotos()
    val photoCountTopic = getOutputTopic[String, Long]("photo_count")
    photoCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "stella" -> 2,
      "colin"  -> 1
    )
  }
}
