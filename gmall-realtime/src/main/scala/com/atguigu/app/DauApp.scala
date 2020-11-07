package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constant.GmallConstant
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")
    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))
    //3.消费  Kafka 中的“启动主题日志” 的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    //打印Value
    //    kafkaDStream.foreachRDD(rdd => { //遍历每一个RDD，我个人可以理解为遍历每一个采集周期的DStream
    //      rdd.foreach(record => { //遍历RDD里面的每一个元素
    //        println(record.value())
    //      })
    //    })




    //4.将每一行数据（就是value）转换为样例类对象,并补充时间字段
    //这里是写在driver端（就是写在rdd外面），因为这是可序列化的，因为implements Serializable
    //因为可序列化，所以可以写在driver，然后在executor端使用
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    //从kafka消费到的原始数据，是个对象
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      //a.获取Value
      val value: String = record.value()
      //b.取出时间戳字段。scala中反射，运行时类获取 classOf(类名)，就可以映射为Dau对象了
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      val ts: Long = startUpLog.ts //时间戳
      //c.将时间戳转换为字符串。年月日+小时
      val dateHourStr: String = sdf.format(new Date(ts))
      //d.给样例类重新赋值时间字段
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)
      //e.返回数据
      startUpLog
    })








    //5.根据Redis进行跨批次去重
    val filterdByRedis: DStream[StartUpLog] =
      DauHandler.filterByRedis(startLogDStream, ssc.sparkContext) //广播变量，和sparkContext有关

    //    startLogDStream.cache() ①复用的rdd和DStream加cache做缓存；②kafka数据被消费走了就没有了，所以加个cache。从缓存里面取数据
    //    startLogDStream.count().print() 原始数据有多少条
    //    filterdByRedis.cache()
    //    filterdByRedis.count().print()过滤之后数据有多少条


    //6.同批次去重(根据Mid分组，然后取时间最小的那一条数据就行)  经过第一次去重后的数据
    val filterdByMid: DStream[StartUpLog] =
      DauHandler.filterByMid(filterdByRedis)

    //    filterdByMid.cache()
    //    filterdByMid.count().print()

    //7.将去重之后的数据中的Mid保存到Redis(为了后续跨批次去重)
    DauHandler.saveMidToRedis(filterdByMid)

    //8.将去重之后的数据明细写入Pheonix
    //saveToPhoenix是一个隐式，隐式就相当于给rdd增加了功能

    filterdByMid.foreachRDD(rdd => {
      //      val configuration: Configuration = HBaseConfiguration.create()
      //      configuration.set("phoenix.schema.isNamespaceMappingEnabled", "true")
      rdd.saveToPhoenix("GMALL200621_DAU",
        //列名，就是建表的字段名
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        //相当于访问地址
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })



    //开启任务
    ssc.start()
    ssc.awaitTermination()


  }
}
