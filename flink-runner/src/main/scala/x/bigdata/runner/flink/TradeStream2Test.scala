package x.bigdata.runner.flink

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TradeStream2Test {

  // nc -lk 9999

  def main(args: Array[String]): Unit = {
    // 输入
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("localhost", 9999, '\n')

    this.test1(stream)

    env.execute("Trade Stream Test is running")

  }

  private def test1(stream: DataStream[String]): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // 转换
    val s1 = stream
      .map(_.split(","))
      .map(x => Trade(x(0), df.parse(x(1)).getTime, x(2).toDouble))

    // 计算
    val s2 = s1
      .assignTimestampsAndWatermarks(new MyWaterMark)
      .keyBy("name")
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .trigger(new MyTrigger)
      .apply(new MyWindowFunction)

    // 输出
    s2.print()

    // 输入数据
    //lh,2019-09-30 16:42:45,1.5
    //lh,2019-09-30 16:42:45,1.6
    //lh,2019-09-30 16:42:45,1.7
    //lh,2019-09-30 16:42:45,1.5
    //lh,2019-09-30 16:43:22,1.9
    //lh,2019-09-30 16:43:22,1.8
    //
    // 输出结果
    //lh,2019-09-30 16:42:40,1,1.5,1.5
    //lh,2019-09-30 16:42:40,2,3.1,1.55
    //lh,2019-09-30 16:42:40,3,4.8,1.5999999999999999
    //lh,2019-09-30 16:42:40,4,6.3,1.575
    //lh,2019-09-30 16:43:20,1,1.9,1.9
    //lh,2019-09-30 16:43:20,2,3.7,1.85

  }

  class MyWindowFunction extends WindowFunction[Trade, String, Tuple, TimeWindow] {

    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Trade], out: Collector[String]): Unit = {
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      var count = 0L
      var sum = 0.0D

      for (in <- input) {
        count = count + 1
        sum = sum + in.amount
      }

      var avg = sum./(count)
      var time = df.format(new Date(window.getStart))

      out.collect(s"${key.getField(0)},$time,$count,$sum,$avg")

    }
  }

  class MyTrigger extends Trigger[Object, TimeWindow] {

    override def onElement(element: Object, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.FIRE
    }

    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      ctx.deleteEventTimeTimer(window.maxTimestamp)
    }

  }

  class MyWaterMark extends AssignerWithPeriodicWatermarks[Trade] {
    var currentMax = 0L;
    val maxTimeLag = 5000L;

    override def getCurrentWatermark: Watermark = {
      new Watermark(currentMax - maxTimeLag)
    }

    override def extractTimestamp(element: Trade, previousElementTimestamp: Long): Long = {
      currentMax = Math.max(element.times, currentMax)
      element.times
    }
  }

  case class Trade(name: String, times: Long, amount: Double)

  //  case class Result(name: String, times: String, counts: Long, sums: Double, avgs: Double)

}
