package org

import org.apache.flink.streaming.api.scala.DataStream

import java.lang

package object kindofdev {
  type Timestamp = lang.Long

  implicit class DataStreamOps[T](ds: DataStream[T]) {
    def nameAndUid(name: String, uid: String): DataStream[T] = ds.name(name).uid(uid)
    def nameAndUid(nameAndUid: String): DataStream[T] = ds.name(nameAndUid).uid(nameAndUid)
  }
}
