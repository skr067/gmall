package com.kqkj.gmall.realtime.util

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertiesName:String):Properties={
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8"))
    prop
  }

}
