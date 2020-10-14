package tw.idv.idiotech.demo.kafkastreams

import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.StreamsBuilder
import goggles._
import tw.idv.idiotech.kafkastreams.avro.StreamsImplicits

trait Streams extends AvroSerdes with StreamsImplicits {

  type Uploader = String
  type FollowerId = String
  type InfluencerId = String
  type UserId = String
  type PhotoId = String
  
  def topology(): Topology = {

    val builder = new StreamsBuilder()

    val userTable: KTable[UserId, User] = builder.table("users")
    val photoTable: KTable[PhotoId, Photo] = builder.table[PhotoId, Photo]("photos")
    val photoStream: KStream[PhotoId, Photo] = photoTable.toStream
    val followTable: KTable[FollowPair, FollowStatus] = builder.table("follows")
    val followersByInfluencer: KTable[InfluencerId, FollowList] = followTable
      .groupBy((k, v) => (k.influencerId, Follow(k, v)))
      .aggregate(
        FollowList(Nil)
      )(
        (_, follow, agg) => agg.copy(data = (follow :: agg.data).sortBy(_.followInfo.time)),
        (_, follow, agg) => agg.copy(data = agg.data.filterNot(_ == follow))
      )

    val photoByUploaderStream: KStream[Uploader, Photo] =
      photoStream.selectKey((_, v) => v.uploader)

    val denormalizedPhotoByUploader: KStream[Uploader, DenormalizedPhoto] = photoByUploaderStream
      .join(userTable)(DenormalizedPhoto.apply)

    val denormalizedPhotoByFollower: KStream[FollowerId, DenormalizedPhoto] =
      denormalizedPhotoByUploader
        .join(followersByInfluencer)(
          (photoAndUser, followers) => FollowersOfPhoto(photoAndUser, followers.data)
        )
        .flatMap(
          (_, v) =>
            v.followers.map(
              f =>
                (
                  f.followPair.followerId,
                  set"${v.photoAndUser}.photo.deleted" :=
                    (if (v.photoAndUser.photo.deleted) true else f.followInfo.deleted)
                )
            )
        )

    def addToTimeline(
      newPosts: List[DenormalizedPhoto],
      photoList: List[DenormalizedPhoto]
    ): List[DenormalizedPhoto] =
      newPosts.foldLeft(photoList)(
        (list, photoAndUser) =>
          if (photoAndUser.photo.deleted) list.filterNot(_.photo.id == photoAndUser.photo.id)
          else (photoAndUser :: list).sortBy(d => 0 - d.photo.timestamp)
      )

    val timelineTable: KTable[FollowerId, Timeline] =
      denormalizedPhotoByFollower.groupByKey.aggregate(Timeline(Nil))(
        (_, p, agg) => agg.copy(data = addToTimeline(List(p), agg.data))
      )

    timelineTable.toStream.to("timeline")

    val photosByUploaderTable: KTable[Uploader, Timeline] =
      denormalizedPhotoByUploader.groupByKey.aggregate(Timeline(Nil))(
        (k, v, agg) => agg.copy(data = v :: agg.data)
      )

    val photosForFollowerStream: KStream[String, PhotosForFollower] = followTable.toStream
      .map((k, v) => (k.influencerId, Follow(k, v)))
      .join(photosByUploaderTable)(
        (follow, timeline) =>
          PhotosForFollower(
            if (!follow.followInfo.deleted) timeline.data
            else timeline.data.map(n => n.copy(photo = n.photo.copy(deleted = true))),
            follow.followPair.followerId
          )
      )
      .selectKey((_, v) => v.followerId)

    val timelineTableFromTopic = builder.table[UserId, Timeline]("timeline")

    val photosFromNewInfluencer = photosForFollowerStream.leftJoin(timelineTableFromTopic)(
      (photosForFollower, timeline) =>
        Timeline(
          addToTimeline(photosForFollower.data, if (timeline == null) Nil else timeline.data)
        )
    )

    photosFromNewInfluencer.to("timeline")

    val followerCount: KTable[String, Long] =
      followTable.filter((k, v) => !v.deleted).groupBy((k, v) => (k.influencerId, v)).count()
    followerCount.toStream.to("follower_count")

    val influencerCount: KTable[String, Long] =
      followTable.filter((k, v) => !v.deleted).groupBy((k, v) => (k.followerId, v)).count()
    influencerCount.toStream.to("influencer_count")

    val photoCount: KTable[String, Long] =
      photoTable.filter((k, v) => !v.deleted).groupBy((k, v) => (v.uploader, v)).count()
    photoCount.toStream.to("photo_count")
    builder.build()
  }

}
