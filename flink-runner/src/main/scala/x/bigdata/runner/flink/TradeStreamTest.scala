package x.bigdata.runner.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.shaded.org.joda.time.DateTime


object TradeStreamTest {
  // nc -lk 9999

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("localhost", 9999, '\n')
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    stream
      .map(_.split(","))
      .map(x => Trade(x(0), df.parse(x(1)).getTime, x(2).toDouble))
      .assignTimestampsAndWatermarks(new MyWaterMark2)
      .keyBy("name")
      //.timeWindow(Time.seconds(20), Time.seconds(10))
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .sum("amount")
      .print()


    env.execute("Trade Stream Test is running")

  }

  case class Trade(name: String, times: Long, amount: Double)

  class MyWaterMark1 extends AssignerWithPeriodicWatermarks[Trade] {
    val maxTimeLag = 5000L;

    override def getCurrentWatermark: Watermark = {
      new Watermark(System.currentTimeMillis() - maxTimeLag)
    }

    override def extractTimestamp(element: Trade, previousElementTimestamp: Long): Long = {
       element.times
    }
  }

  class MyWaterMark2 extends AssignerWithPeriodicWatermarks[Trade] {
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

  //测试数据
  //lh,2019-09-30 16:42:45,1.5
  //lh,2019-09-30 16:43:22,1.6
  //lh,2019-09-30 16:44:22,1.7
  //lh,2019-09-30 16:45:22,1.8

  //结果
  //Trade(lh,1569832965000,1.5)
  //Trade(lh,1569833002000,1.6)
  //Trade(lh,1569833062000,1.7)
  //下一条输入后，上一条才输出

  //规律还没摸清楚，参考EventTimeTrigger

}
