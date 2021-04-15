package com.baiyi.education.etl

import org.apache.flink.api.java.utils.ParameterTool

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object WordCount {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host:String=params.get("host")
    val port:Int=params.getInt("port")

    //创建流处理环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //创建socket流
   import  org.apache.flink.api.scala._

    val txtDstream : DataStream[String] = env.socketTextStream(host, port)

    val dataStream: DataStream[(String, Int)] = txtDstream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty).map((_, 1))
      .keyBy(0)
      //.timeWindow(Time.seconds(5))
      .sum(1)



    dataStream.print("wordcount")
    env.execute("wordcount job")
  }

}
