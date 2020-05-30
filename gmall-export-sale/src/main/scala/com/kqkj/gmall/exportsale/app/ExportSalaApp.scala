package com.kqkj.gmall.exportsale.app

import com.kqkj.gmall.common.constant.GmallConstant
import com.kqkj.gmall.common.util.EsUtil
import com.kqkj.gmall.exportsale.bean.SaleDetailDaycount
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object ExportSalaApp {
  def main(args: Array[String]): Unit = {
    var date = ""
    if (args!=null && args.length>0){
      date = args(0)
    } else {
      date = "2019-03-23"
    }
    val sparkConf = new SparkConf().setAppName("sale_app").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._
    val saleDetailDaycountRDD: RDD[SaleDetailDaycount] = sparkSession.sql("select * from dws_scal_daycount where dt='"+date+"'").as[SaleDetailDaycount].rdd
    saleDetailDaycountRDD.foreachPartition(salaItr => {
      var listBuffer = ListBuffer()
      for (salaDetail <- salaItr){
        listBuffer += salaDetail
        if(listBuffer.size == 100){
          EsUtil.excuteIndexBulk(GmallConstant.ES_INDEX_SALE,listBuffer.toList)
          listBuffer.clear()
        }
      }
      if(listBuffer.size>0){
        EsUtil.excuteIndexBulk(GmallConstant.ES_INDEX_SALE,listBuffer.toList)
      }
    })
  }


}
