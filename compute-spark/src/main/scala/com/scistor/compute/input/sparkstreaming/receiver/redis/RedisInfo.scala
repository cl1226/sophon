package com.scistor.compute.input.sparkstreaming.receiver.redis

case class RedisInfo(config: java.util.Map[String, AnyRef]) {
  var host: String = config.get("host").toString
  var password: String = config.get("password").toString
  var prefKey: String = config.get("prefKey").toString
  var queue: String = config.get("queue").toString
  var maxTotal: Int = config.get("maxTotal").toString.toInt
  var maxIdle: Int = config.get("maxIdle").toString.toInt
  var maxWaitMillis: Long = config.get("maxWaitMillis").toString.toLong
  var connectionTimeout: Int = config.get("connectionTimeout").toString.toInt
  var soTimeout: Int = config.get("soTimeout").toString.toInt
  var maxAttempts: Int = config.get("maxAttempts").toString.toInt
}
