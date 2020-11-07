package com.atguigu.utils
import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

//使用redis时，用工具类获取连接；记住要好借好还
object RedisUtil {
  //声明JedisPool连接池对象为空
  var jedisPool: JedisPool = _

  //提供个方法来获取jedis客户端
  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      println("开辟一个连接池")
      val config: Properties = PropertiesUtil.load("config.properties")
      //读取redis主机和端口
      val host: String = config.getProperty("redis.host")
      val port: String = config.getProperty("redis.port") //string类型，后面转成int类型

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数，如果超过最大连接数，后面就要等待；也就是用完得还，不还就卡死
      jedisPoolConfig.setMaxIdle(20) //最大空闲。没有人用的时候，我维护多少个连接；超过了，就往上增，最大到上面参数的100个连接
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      //创建连接池对象，接下来连接有了，就要开始连接redis,
      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)//连接池配置参数;连接redis哪台机器、端口
    }
//    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
//    println("获得一个连接")

    jedisPool.getResource //从池子中获取一个连接；并返回val resource: Jedis = jedisPool.getResource
  }
}
