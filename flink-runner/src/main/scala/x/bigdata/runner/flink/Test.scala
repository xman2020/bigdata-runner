package x.bigdata.runner.flink

import java.text.SimpleDateFormat

object Test {

  def main(args: Array[String]): Unit = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = df.parse("2019-09-30 16:43:22").getTime
    print(dt)
  }

}
