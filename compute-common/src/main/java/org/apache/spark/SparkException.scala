package org.apache.spark

class SparkException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}

/**
  * Exception thrown when execution of some user code in the driver process fails, e.g.
  * accumulator update fails or failure in takeOrdered (user supplies an Ordering implementation
  * that can be misbehaving.
  */
private[spark] class SparkDriverExecutionException(cause: Throwable)
  extends SparkException("Execution error", cause)

/**
  * Exception thrown when the main user code is run as a child process (e.g. pyspark) and we want
  * the parent SparkSubmit process to exit with the same exit code.
  */
private[spark] case class SparkUserAppException(exitCode: Int)
  extends SparkException(s"User application exited with $exitCode")