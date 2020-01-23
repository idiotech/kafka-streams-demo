package tw.idv.idiotech.demo.kafkastreams

import org.apache.kafka.streams.TopologyTestDriver
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import scala.collection.JavaConverters._

class StreamsSpec extends AnyFlatSpec with Matchers with  MockKafkaStreams with Streams {

  val stella = User("stella", "Stella Hackworth")
  val colin = User("colin", "Colin Codesmith")
  val stalker = User("stalker", "Stalker")

  val blueSunset = Photo("photo-1", "http://blue.sunset/image", stella.id, false)
  val blueFlower = Photo("photo-2", "http://blue.flower/image", stella.id, false)
  val lonelyFigure = Photo("photo-3", "http://lonely.figure/image", colin.id, false)

  val hackagramTopology = topology()

  import tw.idv.idiotech.demo.kafkastreams.avro.StreamsImplicits._

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
    followInput.pipeInput(FollowPair(stella.id, stalker.id), FollowStatus(System.currentTimeMillis(), false))
    followInput.pipeInput(FollowPair(colin.id, stalker.id), FollowStatus(System.currentTimeMillis(), false))
  }

  "streams" must "create timeline for user" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()
    producePhotos()

    val timelineOutput = getOutputTopic[String, Timeline]("timeline")
    val results = timelineOutput.readKeyValuesToMap().asScala
    println(results)
  }

  it must "delete image from timeline" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()
    producePhotos()
    val timelineOutput = getOutputTopic[String, Timeline]("timeline")
    println(timelineOutput.readKeyValue())
    println(timelineOutput.readKeyValue())
    val photoInput = getInputTopic[String, Photo]("photos")
    photoInput.pipeInput(lonelyFigure.id, lonelyFigure.copy(deleted = true))
    println(timelineOutput.readKeyValue())
  }

  it must "delete images from a user who is unfollowed" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()
    producePhotos()
    val timelineOutput = getOutputTopic[String, Timeline]("timeline")
    println(timelineOutput.readKeyValue())
    println(timelineOutput.readKeyValue())
    val followInput = getInputTopic[FollowPair, FollowStatus]("follows")
    followInput.pipeInput(FollowPair(colin.id, stalker.id), FollowStatus(System.currentTimeMillis(), true))
    println(timelineOutput.readKeyValue())
  }

  it must "add photos to timeline when the user follows another user" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    producePhotos()
    produceFollowers()
    val timelineOutput = getOutputTopic[String, Timeline]("timeline")
    println(timelineOutput.readKeyValue())
    println(timelineOutput.readKeyValue())
  }

  it must "calculate follower counts" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()

    val followInput = getInputTopic[FollowPair, FollowStatus]("follows")
    followInput.pipeInput(FollowPair(stella.id, colin.id), FollowStatus(System.currentTimeMillis(), false))

    val followerCountTopic = getOutputTopic[String, Long]("follower_count")
    followerCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "stella" -> 2,
      "colin" -> 1
    )

    val influencerCountTopic = getOutputTopic[String, Long]("influencer_count")
    influencerCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "colin" -> 1,
      "stalker" -> 2
    )
  }

  it must "correctly recompute followers count in case of an unfollow" in {
    implicit val testDriver: TopologyTestDriver = createTestDriver(hackagramTopology)
    produceUsers()
    produceFollowers()

    val followInput = getInputTopic[FollowPair, FollowStatus]("follows")
    followInput.pipeInput(FollowPair(stella.id, colin.id), FollowStatus(System.currentTimeMillis(), false))
    followInput.pipeInput(FollowPair(stella.id, colin.id), FollowStatus(System.currentTimeMillis(), true))

    val followerCountTopic = getOutputTopic[String, Long]("follower_count")
    followerCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "stella" -> 1,
      "colin" -> 1
    )

    val influencerCountTopic = getOutputTopic[String, Long]("influencer_count")
    influencerCountTopic.readKeyValuesToMap().asScala mustBe Map(
      "colin" -> 0,
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
      "colin" -> 1
    )
  }
}
