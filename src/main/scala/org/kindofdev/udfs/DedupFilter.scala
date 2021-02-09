package org.kindofdev.udfs

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.functions.KeySelector
import org.kindofdev.log.Logging

import java.time.Duration
import scala.reflect.ClassTag

class DedupFilter[T: ClassTag, K: ClassTag](ttl: Duration, keySelector: KeySelector[T, K]) extends RichFilterFunction[T] with Logging {

  private val KClass: Class[K] = implicitly[ClassTag[K]].runtimeClass.asInstanceOf[Class[K]]
  private val TClass: Class[T] = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

  private val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(ttl.getSeconds))
    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build

  private val mapDedupStateDescriptor = new MapStateDescriptor[K, Boolean](s"dedup $TClass", KClass, classOf[Boolean])
  mapDedupStateDescriptor.enableTimeToLive(ttlConfig)

  private lazy val dedupMapState: MapState[K, Boolean] = getRuntimeContext.getMapState(mapDedupStateDescriptor)

  override def filter(value: T): Boolean = {
    val key = keySelector.getKey(value)
    val seen = dedupMapState.get(key)
    if (!seen) {
      dedupMapState.put(key, true)
      true
    } else {
      logger.warn(s"Duplicated event detected for key [{}]", key)
      false
    }
  }
}
