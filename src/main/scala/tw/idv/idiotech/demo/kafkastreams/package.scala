package tw.idv.idiotech.demo

package object kafkastreams {

  case class Follow(followPair: FollowPair, followInfo: FollowStatus)
  case class FollowList(data: List[Follow])
  case class DenormalizedPhoto(photo: Photo, user: User)
  case class FollowersOfPhoto(photoAndUser: DenormalizedPhoto, followers: List[Follow])
  case class Timeline(data: List[DenormalizedPhoto])
  case class PhotosForFollower(data: List[DenormalizedPhoto], followerId: String)

}
