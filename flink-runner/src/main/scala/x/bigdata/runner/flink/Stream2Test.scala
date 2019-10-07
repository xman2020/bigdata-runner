package x.bigdata.runner.flink

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.shaded.org.joda.time.DateTime
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

  case class Login(name: String, times: Timestamp, status: String)

  def loginCountSql(stream: DataStream[String]): Unit = {
    stream.print();

    val tableEnv = StreamTableEnvironment.create(stream.executionEnvironment)
    val stream1 = stream.map(_.split(",")).map(x => Login(x(0), new Timestamp(DateTime.parse(x(1)).getMillis()), x(2)))
    tableEnv.registerDataStream("login", stream1)
    val result = tableEnv.sqlQuery(
      "select name, count(*), HOP_ROWTIME(times, INTERVAL '20' SECOND, INTERVAL '10' SECOND) " +
      "from login " +
      "group by name, HOP(times, INTERVAL '20' SECOND, INTERVAL '10' SECOND)")
    val stream2 = tableEnv.toRetractStream[Row](result)
    stream2.print()


    //报错：Exception in thread "main" org.apache.flink.table.api.ValidationException:
    // Window can only be defined over a time attribute column.
  }

}
