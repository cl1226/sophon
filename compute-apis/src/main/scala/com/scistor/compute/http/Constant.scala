package com.scistor.compute.http

object Constant {

  val iNetPort: Int = 80

  val mappingPackage = "com.scistor.compute.service"

  val MIN_BUFFER_SIZE = 51200
  val INITIAL_BUFFER_SIZE = 102400
  val MAXIMUM_BUFFER_SIZE: Int = 102400 * 10

  val NOT_FOUND: String =
    """
      |.__   __.   ______   .___________.    _______   ______    __    __  .__   __.  _______
      ||  \ |  |  /  __  \  |           |   |   ____| /  __  \  |  |  |  | |  \ |  | |       \
      ||   \|  | |  |  |  | `---|  |----`   |  |__   |  |  |  | |  |  |  | |   \|  | |  .--.  |
      ||  . `  | |  |  |  |     |  |        |   __|  |  |  |  | |  |  |  | |  . `  | |  |  |  |
      ||  |\   | |  `--'  |     |  |        |  |     |  `--'  | |  `--'  | |  |\   | |  '--'  |
      ||__| \__|  \______/      |__|        |__|      \______/   \______/  |__| \__| |_______/
      |
      """.stripMargin

}
