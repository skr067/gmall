package com.kqkj.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.kqkj.gmall.common.constant.GmallConstant
import com.kqkj.gmall.common.util.EsUtil
import com.kqkj.gmall.realtime.bean.StartUpLog
import com.kqkj.gmall.realtime.util.{KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.immutable


object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc,Seconds(5))

    val startupStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)
    /*startupStream.foreachRDD(rdd => {
      println(rdd.map(_.value()).collect().mkString("\n"))
    })*/
    val startUpLogStream: DStream[StartUpLog] = startupStream.map(log => {
      val jsonStr = log.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])
      val date = new Date(startUpLog.ts)
      val dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
      val dateArr = dateStr.split(" ")
      startUpLog.logDate = dateArr(0)
      startUpLog.logHour = dateArr(1).split(":")(0)
      startUpLog.logHourMinute = dateArr(1)
      startUpLog
    })
    //利用redis进行去重过滤
          //方式一
    /*startUpLogStream.filter(startuplog => {
      val jedis = RedisUtil.getJedisClient
      val key = "dau:"+ startuplog.logDate
      !jedis.sismember(key,startuplog.mid)
    })*/
          //方式二
    /*val jedis = RedisUtil.getJedisClient
    val curdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
    val key = "dau:"+ curdate
    val dauSet: util.Set[String] = jedis.smembers(key)//取出全部value值
    val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)//广播变量
    startUpLogStream.filter(startuplog => {
      val dauSet = dauBC.value
      !dauSet.contains(startuplog.mid)
    })*/
          //方式三
    val filteredDstream = startUpLogStream.transform(rdd => {
      //rdd-to-rdd
      //driver(周期性执行)
      val jedis = RedisUtil.getJedisClient
      val curdate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key = "dau:" + curdate
      val dauSet: util.Set[String] = jedis.smembers(key)
      //取出全部value值
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      //广播变量
      val filteredRDD = rdd.filter(startuplog => {
        //excutor(执行一次)
        val dauSet = dauBC.value
        !dauSet.contains(startuplog.mid)
      })
      filteredRDD
    })
    //每批次自查去重（把相同的mid数据分成一组，每组取第一个）
    val groupByMidDstream = filteredDstream.map(startuplog =>(startuplog.mid,startuplog)).groupByKey()
    val distinctDstream: DStream[StartUpLog] = groupByMidDstream.flatMap{case (mid,startlogItr) => startlogItr.take(1)}
    //保存到redis中去
    distinctDstream.foreachRDD(rdd =>{//driver
      rdd.foreachPartition(startuplogItr=>{//迭代器只能使用一次
        val jedis: Jedis = RedisUtil.getJedisClient
        val list: List[StartUpLog] = startuplogItr.toList
        for (startuplog <- list){
          //redis : type set（不重复）
          //key  dau:yyyy-MM-ss value:mids
          val key = "dau:"+ startuplog.logDate
          val value = startuplog.mid
          jedis.sadd(key,value)
        }
        //往es里存
        EsUtil.excuteIndexBulk(GmallConstant.ES_INDEX_DAU,list)
        jedis.close()
      })
      /*rdd.foreach(startuplog =>{//excutor
        val jedis: Jedis = RedisUtil.getJedisClient
        //redis : type set
        //key  dau:yyyy-MM-ss value:mids
        val key = "dau:"+ startuplog.logDate
        val value = startuplog.mid
        jedis.sadd(key,value)
        jedis.close()
      })*/
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
