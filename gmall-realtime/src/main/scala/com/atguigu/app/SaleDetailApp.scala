package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.constant.GmallConstant
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

//将OrderInfo与OrderDetail数据进行双流JOIN,并根据user_id查询Redis,补全用户信息
object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka订单以及订单明细主题数据创建流
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]]
    = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_INFO, ssc)

    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]]
    = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER_DETAIL, ssc)

    //4.将数据转换为样例类对象并转换结构为KV。必须key-vlaue类型才能做join
    //orderInfo
    val orderIdToInfoDStream: DStream[(String, OrderInfo)]
    = orderInfoKafkaDStream.map(record => {
      //a.将value转换为样例类
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //b.取出创建时间 yyyy-MM-dd HH:mm:ss
      val create_time: String = orderInfo.create_time
      //c.给时间重新赋值
      val dateTimeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)
      //d.数据脱敏
      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      //e.返回结果
      (orderInfo.id, orderInfo)
    })

    //orderDetail
    val orderIdToDetailDStream: DStream[(String, OrderDetail)]
    = orderDetailKafkaDStream.map(record => {
      //a.转换为样例类
      val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      //b.返回数据
      (detail.order_id, detail)
    })

    //双流JOIN(普通JOIN)
    //    val value: DStream[(String, (OrderInfo, OrderDetail))]
    //    = orderIdToInfoDStream.join(orderIdToDetailDStream)
    //    value.print(100)


    //5.全外连接 由于我们使用了redis，所以不能一条连接一条连接的join
    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))]
    = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //6.处理JOIN之后的数据  分区操作代替单条数据操作，减少与redis连接
    //mapPartitions会有返回数据集。
    fullJoinDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放         能够关联上的数据
      val details = new ListBuffer[SaleDetail]

      //遍历iter,做数据处理
      iter.foreach { case ((orderId, (infoOpt, detailOpt))) =>
        //创建两个redisKey  进行拼接
        val infoRedisKey = s"OrderInfo:$orderId"
        val detailRedisKey = s"OrderDetail:$orderId"

        if (infoOpt.isDefined) {
          //a.判断infoOpt不为空
          //取出infoOpt数据
          val orderInfo: OrderInfo = infoOpt.get //不为空，直接get获取数据

          //a.1 判断detailOpt不为空
          if (detailOpt.isDefined) {
            //取出detailOpt数据
            val orderDetail: OrderDetail = detailOpt.get
            //封装到SaleDetail对象
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            //添加至集合，最后把集合返回
            details :+ saleDetail
          }

          //a.2 完成第二天分支先---> 将info数据写入redis,给后续的detail数据使用
          //总不能把对象写入到redis中去，所以要先转换成json字符串。
          // val infoStr: String = JSON.toJSONString(orderInfo)//把json对象转换成字符串。但是这里编译通不过
          //之前我们把java的集合list、map都通过这种方式转换成json字符串。但这里的orderInfo是一个scala样例类对象，这样行不通

          import org.json4s.native.Serialization //导包
          //formats这个隐式对象是不可序列化的，所以不能放到外面。作为转换json的一种方式
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats//导入依赖后，生成个format对象。隐式对象
          val infoStr: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, infoStr)

          //a.3

        } else {
          //b.判断infoOpt为空
        }

      }

      //归还连接
      jedisClient.close()

      //最终返回值。把details集合返回，但是最后要的是迭代器结构，转换结构进行返回
      //一个分区的数据添加完之后，做整体返回
      details.iterator

    })


    //测试
    //    orderInfoKafkaDStream.foreachRDD(rdd=>{
    //      rdd.foreach(record=>println(record.value()))
    //    })
    //    orderDetailKafkaDStream.foreachRDD(rdd=>{
    //      rdd.foreach(record=>println(record.value()))
    //    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
