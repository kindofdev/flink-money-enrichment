package org.kindofdev.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

class SplitterFunction[T: TypeInformation, K](conditionMap: Map[OutputTag[T], T => Boolean]) extends KeyedProcessFunction[K, T, Unit] {
  override def processElement(value: T, ctx: KeyedProcessFunction[K, T, Unit]#Context, out: Collector[Unit]): Unit = {
    conditionMap.foreach {
      case (outputTag, fun) => if (fun(value)) ctx.output(outputTag, value)
    }
  }
}
