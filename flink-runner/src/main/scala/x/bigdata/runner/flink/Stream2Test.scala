package x.bigdata.runner.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

object Stream2Test {
  // 对应spark-runner/StructStreamTest

  // nc -lk 9999

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("localhost", 9999, '\n')

    this.loginCountSql(stream)

    env.execute("Stream2Test is running")
  }

  case class Login(name: String, time: String, status: String)


  def loginCountSql(stream: DataStream[String]): Unit = {
    stream.print();

    val tableEnv = StreamTableEnvironment.create(stream.executionEnvironment)
    val stream1 = stream.map(_.split(",")).map(x => Login(x(0), x(1), x(2)))
    tableEnv.registerDataStream("login", stream1)
    val result = tableEnv.sqlQuery(
      "select name, count(*), HOP_ROWTIME(time, INTERVAL '20' SECOND, INTERVAL '10' SECOND) " +
      "from login " +
      "group by name, HOP(time, INTERVAL '20' SECOND, INTERVAL '10' SECOND)")
    val stream2 = tableEnv.toRetractStream[Row](result)
    stream2.print()

  }


}
