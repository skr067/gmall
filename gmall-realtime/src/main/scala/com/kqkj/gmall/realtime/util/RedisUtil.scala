package com.kqkj.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {
  var jedisPool:JedisPool = null

  def getJedisClient:Jedis = {
    if(jedisPool == null){
      //开辟一个连接池
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(50)//最大连接数
      jedisPoolConfig.setMaxIdle(10)//最大空闲
      jedisPoolConfig.setMinIdle(10)//最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)//忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长
      jedisPoolConfig.setTestOnBorrow(true)//每次获得连接的进行测试
      jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)
    }
    jedisPool.getResource

  }

}
