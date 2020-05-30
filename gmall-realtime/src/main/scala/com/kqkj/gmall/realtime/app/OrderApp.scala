package com.kqkj.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.kqkj.gmall.common.constant.GmallConstant
import com.kqkj.gmall.common.util.EsUtil
import com.kqkj.gmall.realtime.bean.OrderInfo
import com.kqkj.gmall.realtime.util.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("oder_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    //保存到es
    //数据脱敏 补充时间戳
    val inputDstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER,ssc)
    val orderInfoDstream = inputDstream.map(record => {
      val jsonstr = record.value()
      val orderInfo = JSON.parseObject(jsonstr, classOf[OrderInfo])
      val telSplit = orderInfo.consigneeTel.splitAt(4)
      orderInfo.consigneeTel = telSplit._1 + "*******"
      val datetimeArr = orderInfo.createTime.split(" ")
      orderInfo.createDate = datetimeArr(0)
      val timeArr = datetimeArr(1).split(":")
      orderInfo.createHour = timeArr(0)
      orderInfo.createHourMinute = timeArr(0) + ":" + timeArr(1)
      orderInfo
    })
    orderInfoDstream.foreachRDD{rdd =>
      rdd.foreachPartition{orderItr =>{
        EsUtil.excuteIndexBulk(GmallConstant.ES_INDEX_ORDER,orderItr.toList)
      }
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }

}
