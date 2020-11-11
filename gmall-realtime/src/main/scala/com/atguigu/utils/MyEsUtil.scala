package com.atguigu.utils

import java.util
import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}
import collection.JavaConverters._

object MyEsUtil {

  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _

  /**
   * 获取客户端对象
   * @return jestclient
   */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
   * 关闭客户端
   */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
   * 建立连接
   */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
      .multiThreaded(true) //多线程
      .maxTotalConnection(200) //连接总数
      .connTimeout(10000)  //连接超时时间
      .readTimeout(10000)
      .build)
  }

  // 批量插入数据到ES，把ES作为存储数据库
  //indexName往哪个索引里写；docList准备写入的数据，元组第一位docId。第二位index里面的source，我们把source下面的数据封装成样例类对象，传入进去
  def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {

    if (docList.size>0) {

      //获取ES客户端连接
      val jest: JestClient = getClient

      //在循环之前创建Bulk.Builder
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
        .defaultIndex(indexName)
        .defaultType("_doc")

      //循环创建Index对象,并设置进Bulk.Builder
      for ((id, doc) <- docList) {
        val indexBuilder = new Index.Builder(doc)

        if (id != null) {
          indexBuilder.id(id)
        }
        val index: Index = indexBuilder.build() //创建index对象
        bulkBuilder.addAction(index)
      }

      //创建Bulk对象，把一个分区里面的数据都添加进来了
      val bulk: Bulk = bulkBuilder.build()

      var items: util.List[BulkResult#BulkResultItem] = null //java的集合

      try {
        //执行批量写入操作
        items = jest.execute(bulk).getItems
      } catch {
        case ex: Exception => println(ex.toString)
      } finally {
        close(jest)
        println("保存" + items.size() + "条数据")
        for (item <- items.asScala) {
          if (item.error != null && item.error.nonEmpty) { //如果错误信息不为空，那就打印错误信息
            println(item.error)
            println(item.errorReason)
          }
        }
      }
    }
  }
}
