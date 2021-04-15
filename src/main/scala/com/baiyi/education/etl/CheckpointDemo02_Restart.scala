package com.baiyi.education.etl


import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.commons.lang.SystemUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer
object CheckpointDemo02_Restart {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //TODO ===========Checkpoint参数设置====
    //===========类型1:必须参数=============
    //设置Checkpoint的时间间隔为1000ms做一次Checkpoint/其实就是每隔1000ms发一次Barrier!
    env.enableCheckpointing(1000)
    //设置State状态存储介质/状态后端
    //Memory:State存内存,Checkpoint存内存--开发不用!
    //Fs:State存内存,Checkpoint存FS(本地/HDFS)--一般情况下使用
    //RocksDB:State存RocksDB(内存+磁盘),Checkpoint存FS(本地/HDFS)--超大状态使用,但是对于状态的读写效率要低一点
    /*if(args.length > 0){
        env.setStateBackend(new FsStateBackend(args[0]));
    }else {
        env.setStateBackend(new FsStateBackend("file:///D:\\data\\ckp"));
    }*/
    if (SystemUtils.IS_OS_WINDOWS) {
      env.setStateBackend(new FsStateBackend("file:///data/ckp"))
    } else {
      env.setStateBackend(new FsStateBackend("hdfs://192.168.155.175:8020/flink-checkpoint/checkpoint"))
    }
    //===========类型2:建议参数===========
    //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
    //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)//默认是0
    //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败：true是  false不是
    //env.getCheckpointConfig().setFailOnCheckpointingErrors(false);//默认是true
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(10)//默认值为0，表示不容忍任何检查点失败
    //设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：true,当作业被取消时，删除外部的checkpoint(默认值)
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：false,当作业被取消时，保留外部的checkpoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //===========类型3:直接使用默认的即可===============
    //设置checkpoint的执行模式为EXACTLY_ONCE(默认)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //设置checkpoint的超时时间,如果 Checkpoint在 60s内尚未完成说明该次Checkpoint失败,则丢弃。
    env.getCheckpointConfig.setCheckpointTimeout(60000)//默认10分钟
    //设置同一时间有多少个checkpoint可以同时执行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)//默认为1


    //TODO ===配置重启策略:
    //1.配置了Checkpoint的情况下不做任务配置:默认是无限重启并自动恢复,可以解决小问题,但是可能会隐藏真正的bug
    //2.单独配置无重启策略
    //env.setRestartStrategy(RestartStrategies.noRestart());
    //3.固定延迟重启--开发中常用

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,Time.seconds(5)))


    //上面的设置表示:如果job失败,重启3次, 每次间隔5s
    //4.失败率重启--开发中偶尔使用
    /*env.setRestartStrategy(RestartStrategies.failureRateRestart(
            3, // 每个测量阶段内最大失败次数
            Time.of(1, TimeUnit.MINUTES), //失败率测量的时间间隔
            Time.of(3, TimeUnit.SECONDS) // 两次连续重启的时间间隔
    ));*/
    //上面的设置表示:如果1分钟内job失败不超过三次,自动重启,每次重启间隔3s (如果1分钟内程序失败达到3次,则程序退出)


    //2.Source


    val dataStream: DataStream[String] = env.socketTextStream("192.168.155.175",9999)

    //3.Transformation
    //3.1切割出每个单词并直接记为1


    val result: DataStream[String] = dataStream.flatMap(line => {
      val words = line.split(" ")
      words.map(t => {

        if (t.equals("bug")) {
          println("bug....")
          throw new Exception("bug.....")
        }

        (t, 1)
      })

    }).keyBy(_._1)
      .sum(1)
      .map(t => t._1 + "::" + t._2)


    result.print()


   val pros = new Properties()


    pros.setProperty("bootstrap.servers", "192.168.155.179:6667")

    val kafkaSink :FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String]("flink_kafka",new SimpleStringSchema(),pros)

    result.addSink(kafkaSink)



    env.execute()

  }

}
