package com.scistor.compute.output.utils.jdbc

import java.util.Properties

object DBPoolManager {

  var dbManager: DBPool = _

  def getDBPoolManager(prop: Properties): DBPool = {
    synchronized {
      if (dbManager == null) {
        dbManager = new DBPool(prop)
      }
    }
    dbManager
  }

}
