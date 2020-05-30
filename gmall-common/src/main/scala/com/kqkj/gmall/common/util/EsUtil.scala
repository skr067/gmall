package com.kqkj.gmall.common.util

import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}

object EsUtil {

  private val ES_HOST ="http://kqkj101"
  private val ES_HTTP_PORT = 9200
  private var factory:JestClientFactory = null

  /**
    * 获取客户端
    */
  def getClient = {
    if(factory == null) build()
    factory.getObject
  }
  /**
    *关闭客户端
   */
  def close(client : JestClient): Unit ={
    if(!Objects.isNull(client))
      try{
        client.shutdownClient()
      } catch{
        case e:Exception => e.printStackTrace()
      }
  }

  /**
    * 建立连接
    */
  private def build(): Unit ={
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST+":"+ES_HTTP_PORT).multiThreaded(true)
        .maxTotalConnection(20)//连接总数
        .connTimeout(10000).readTimeout(10000).build)
  }

  /**
    * 批量
    */
  def excuteIndexBulk(indexName:String,list: List[Any]): Unit ={
    val jest = getClient
    val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
    for (doc <- list){
      val index = new Index.Builder(doc).build()
      bulkBuilder.addAction(index)
    }
    val items = jest.execute(bulkBuilder.build()).getItems
    println(s"保存=${items.size()}")
    close(jest)
  }
}