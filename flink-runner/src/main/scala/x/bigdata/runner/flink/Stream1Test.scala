package x.bigdata.runner.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

object Stream1Test {
  // 对应spark-runner/StreamTest

  // nc -lk 9999

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.socketTextStream("localhost", 9999, '\n')

    //this.print(stream)

    //this.writeFile(stream)

    //this.wordcount(stream)

    //this.logincount(stream)

    //this.loginWindowcount(stream)

    this.loginCountSql(stream)

    env.execute("Stream1Test is running")
  }

  def loginCountSql(stream: DataStream[String]): Unit = {
    stream.print();

    val tableEnv = StreamTableEnvironment.create(stream.executionEnvironment)
    val stream1  = stream.map(_.split(",")).map(x => Login(x(0), x(1), x(2)))
    tableEnv.registerDataStream("login", stream1)
    val result = tableEnv.sqlQuery("select name, count(*) from login group by name")
    val stream2 = tableEnv.toRetractStream[Row](result)
    stream2.print()

    // SQL模式摸索好久才跑起来，报各种隐式转换的错误（估计是泛型）
    // tableEnv.toAppendStream在SQL使用聚合时会报错，提示使用toRetractStream

    //lh,2019-01-22 11:04:11,success
    //xs,2019-01-22 11:04:11,fail
    //dxd,2019-01-22 11:04:11,success
    //lh,2019-01-22 11:04:11,request
    //zp,2019-01-22 11:04:11,fail
    //xs,2019-01-22 11:04:11,request
    //dxd,2019-01-22 11:04:11,request
    //lh,2019-01-22 11:04:11,request
    //xs,2019-01-22 11:04:11,success
    //dxd,2019-01-22 11:04:11,success
    //(true,lh,1)
    //(true,xs,1)
    //(true,dxd,1)
    //(false,lh,1)
    //(true,lh,2)
    //(true,zp,1)
    //(false,xs,1)
    //(true,xs,2)
    //(false,dxd,1)
    //(true,dxd,2)
    //(false,lh,2)
    //(true,lh,3)
    //(false,xs,2)
    //(true,xs,3)
    //(false,dxd,2)
    //(true,dxd,3)
  }

  case class Login(name: String, time: String, status: String)

  def loginWindowcount(stream: DataStream[String]): Unit = {
    stream.print();

    stream
      .map(_.split(","))
      .map(x => (x(0), 1))
      .keyBy(0)
      .timeWindow(Time.seconds(20), Time.seconds(10))
      .sum(1)
      .print()

    //lh,2019-01-22 11:04:11,success
    //xs,2019-01-22 11:04:11,fail
    //dxd,2019-01-22 11:04:11,success
    //lh,2019-01-22 11:04:11,request
    //zp,2019-01-22 11:04:11,fail
    //-------------------------------------------为了分隔好看，其实没有这条线
    //(lh,2)
    //(xs,1)
    //(zp,1)
    //(dxd,1)
    //-------------------------------------------为了分隔好看，其实没有这条线
    //xs,2019-01-22 11:04:11,request
    //zp,2019-01-22 11:04:11,fail
    //xs,2019-01-22 11:04:11,request
    //lh,2019-01-22 11:04:11,request
    //zp,2019-01-22 11:04:11,fail
    //xs,2019-01-22 11:04:11,success
    //hnx,2019-01-22 11:04:11,success
    //dxd,2019-01-22 11:04:11,request
    //csy,2019-01-22 11:04:11,success
    //-------------------------------------------为了分隔好看，其实没有这条线
    //(dxd,2)
    //(zp,3)
    //(lh,3)
    //(hnx,1)
    //(xs,4)
    //(csy,1)
    //-------------------------------------------为了分隔好看，其实没有这条线
    //(lh,1)
    //(zp,2)
    //(xs,3)
    //(csy,1)
    //(hnx,1)
    //(dxd,1)
  }

  def logincount(stream: DataStream[String]): Unit = {
    stream.print();

    stream
      .map(_.split(","))
      .filter(_ (2) == "success")
      .map(x => (x(0), 1))
      .keyBy(0)
      .sum(1)
      .print()

    //lh,2019-01-2211:04:11,success
    //xs,2019-01-2211:04:11,fail
    //dxd,2019-01-2211:04:11,success
    //lh,2019-01-2211:04:11,request
    //zp,2019-01-2211:04:11,fail
    //xs,2019-01-2211:04:11,request
    //dxd,2019-01-2211:04:11,request
    //lh,2019-01-2211:04:11,request
    //xs,2019-01-2211:04:11,success
    //dxd,2019-01-2211:04:11,success
    //-------------------------------------------为了分隔好看，其实没有这条线
    //(lh,1)
    //(dxd,1)
    //(xs,1)
    //(dxd,2)
  }

  def wordcount(stream: DataStream[String]): Unit = {
    stream.print();

    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()

    //hello world
    //hello hadoop
    //hadoop is a bigdata platform
    //bigdata is a popular technic
    //
    //hadoop is good
    //-------------------------------------------为了分隔好看，其实没有这条线
    //(hello,1)
    //(world,1)
    //(hello,2)
    //(hadoop,1)
    //(hadoop,2)
    //(is,1)
    //(a,1)
    //(bigdata,1)
    //(platform,1)
    //(bigdata,2)
    //(is,2)
    //(a,2)
    //(popular,1)
    //(technic,1)
    //(,1)
    //(hadoop,3)
    //(is,3)
    //(good,1)
  }

  def writeFile(stream: DataStream[String]): Unit = {
    stream.writeAsText("src/main/resources/login_receive")

    // 生成了一个login_receive文本文件，内容和print一样
    // 如果setParallelism=n，会在目录下生成n个文件
  }

  def print(stream: DataStream[String]): Unit = {
    stream.print();

    // 输入回车后，很快在控制台输出

    //lh,2019-01-22 11:04:11,request
    //xs,2019-01-22 11:04:11,fail
    //dxd,2019-01-22 11:04:11,success
  }

}
