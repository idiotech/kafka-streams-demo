package tw.idv.idiotech.demo.kafkastreams

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.StreamsBuilder

trait Streams extends AvroSerdes {

  def topology(): Topology = {

    import tw.idv.idiotech.demo.kafkastreams.avro.StreamsImplicits._

    val builder = new StreamsBuilder()
    type Uploader = String
    type FollowerId = String
    type UserId = String
    type PhotoId = String

    val userTable: KTable[UserId, User] = builder.table("users")
    val photoTable: KTable[String, Photo] = builder.table[String, Photo]("photos")
    val photoStream: KStream[String, Photo] = photoTable.toStream
    val followTable: KTable[FollowPair, FollowStatus] = builder.table("follows")
    val followersByInfluencer: KTable[Uploader, FollowList] = followTable.groupBy((k, v) => (k.influencerId, Follow(k, v))).aggregate(
      FollowList(Nil)
    )(
      (_, follow, agg) => agg.copy(data = (follow :: agg.data).sortBy(_.followInfo.time)),
      (_, follow, agg) => agg.copy(data = agg.data.filterNot(_ == follow))
    )

    val photoByUploaderStream: KStream[Uploader, Photo] = photoStream.selectKey((_, v) => v.uploader)

    val denormalizedPhotoByUploder: KStream[Uploader, DenormalizedPhoto] = photoByUploaderStream
      .join(userTable)(DenormalizedPhoto.apply)

    val denormalizedPhotoByFollower: KStream[FollowerId, DenormalizedPhoto] =
      denormalizedPhotoByUploder
      .join(followersByInfluencer)(
        (photoAndUser, followers) => FollowersOfPhoto(photoAndUser, followers.data)
      )
      .flatMap((k, v) => v.followers.map(f => (
        f.followPair.followerId,
        v.photoAndUser.copy(photo = v.photoAndUser.photo.copy(deleted = if (v.photoAndUser.photo.deleted) true else f.followInfo.deleted))
      )))

    def addToTimeline(newPosts: List[DenormalizedPhoto], photoList: List[DenormalizedPhoto]): List[DenormalizedPhoto] =
      newPosts.foldLeft(photoList)((list, photoAndUser) =>
        if (photoAndUser.photo.deleted) list.filterNot(_.photo.id == photoAndUser.photo.id)
        else (photoAndUser :: list).take(500) // optional // scala sorted list
      )

    val timelineTable: KTable[FollowerId, Timeline] = denormalizedPhotoByFollower
      .groupByKey.aggregate(Timeline(Nil))(
      (_, p, agg) => agg.copy(data = addToTimeline(List(p), agg.data))
    )

    timelineTable.toStream.to("timeline")

    // unfollow: join timeline, delete everything from that user
    // follow: get aggregated photos by user, add all

    val photosByUploaderTable: KTable[Uploader, Timeline] = denormalizedPhotoByUploder
      .groupByKey.aggregate(Timeline(Nil))(
      (k, v, agg) => agg.copy(data = v :: agg.data)
    )

    val photosForFollowerStream: KStream[String, PhotosForFollower] = followTable.toStream.map((k, v) => (k.influencerId, Follow(k, v)))
      .join(photosByUploaderTable)((follow, timeline) => PhotosForFollower(
        if (!follow.followInfo.deleted) timeline.data else timeline.data.map(n => n.copy(photo = n.photo.copy(deleted = true))),
        follow.followPair.followerId)
      )
      .selectKey((_, v) => v.followerId)

    val timelineTableFromTopic = builder.table[String, Timeline]("timeline")

    val photosFromNewInfluencer = photosForFollowerStream.leftJoin(timelineTableFromTopic)(
      (photosForFollower, timeline) => Timeline(addToTimeline(photosForFollower.data, if (timeline == null) Nil else timeline.data))
    )

    photosFromNewInfluencer.to("timeline")

    val followerCount: KTable[String, Long] = followTable.filter((k, v) => !v.deleted).groupBy((k, v) => (k.influencerId, v)).count()
    followerCount.toStream.to("follower_count")

    val influencerCount: KTable[String, Long] = followTable.filter((k, v) => !v.deleted).groupBy((k, v) => (k.followerId, v)).count()
    influencerCount.toStream.to("influencer_count")

    val photoCount: KTable[String, Long] =  photoTable.filter((k, v) => !v.deleted).groupBy((k, v) => (v.uploader, v)).count()
    photoCount.toStream.to("photo_count")
    builder.build()

  }

}
