package com.scistor.compute.common

class RemoteConfig {

  var host: String = _
  var port: Int = _
  var user: String = _
  var password: String = _

  def this(host: String, port: Int = 22, user: String, password: String = "") {
    this()
    this.host = host
    this.port = port
    this.user = user
    this.password = password
  }

}
