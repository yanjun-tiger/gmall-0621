package com.atguigu.utils


import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

//读取配置文件的工具类
object PropertiesUtil {

  //到这个文件加载配置数据，封装到Properties对象里面去
  def load(propertieName: String): Properties = {
    val prop = new Properties()
    //怎么加载配置文件呢？
    // prop.load()表示去加载某一个数据流中的数据
    //怎么样获取流？FileInputFormat指的是本地磁盘文件。但是我们是放在服务器的，不能指定固定路径
    prop.load(new InputStreamReader(
      //获取当前线程。类加载器。
      Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName),
      StandardCharsets.UTF_8))

    prop //要把这对象返回
  }
}

