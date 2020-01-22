package tw.idv.idiotech.demo.kafkastreams

import tw.idv.idiotech.demo.kafkastreams.avro.{KeySerde, ScalaSerdes, ValueSerde}

trait AvroSerdes extends ScalaSerdes {
  implicit def userValueSerde: ValueSerde[User] = valueSerde
  implicit def photoValueSerde: ValueSerde[Photo] = valueSerde
  implicit def followPairKeySerde: KeySerde[FollowPair] = keySerde
  implicit def followInfoValueSerde: ValueSerde[FollowStatus] = valueSerde
  implicit def followValueSerde: ValueSerde[Follow] = valueSerde
  implicit def followListValueSerde: ValueSerde[FollowList] = valueSerde
  implicit def timelineValueSerde: ValueSerde[Timeline] = valueSerde
  implicit def photoListValueSerde: ValueSerde[PhotosForFollower] = valueSerde
  implicit def denormalizedPhotoValueSerde: ValueSerde[DenormalizedPhoto] = valueSerde
  implicit def followersOFPhotoValueSerde: ValueSerde[FollowersOfPhoto] = valueSerde
}
