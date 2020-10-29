package com.scistor.compute.output.batch

import java.text.SimpleDateFormat
import java.util.Calendar

import com.scistor.compute.apis.BaseOutput
import com.scistor.compute.model.spark.SinkAttribute
import org.apache.spark.sql.{DataFrameWriter, Dataset, Row, SaveMode}

abstract class FileOutputBase extends BaseOutput {

  var sink: SinkAttribute = _

  /**
   * Set SinkAttribute.
   **/
  override def setSink(sink: SinkAttribute): Unit = {
    this.sink = sink
  }

  /**
   * get SinkAttribute.
   **/
  override def getSink(): SinkAttribute = {
    this.sink
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def validate(): (Boolean, String) = {
    (true, "")
  }

  protected def fileWriter(df: Dataset[Row]): DataFrameWriter[Row] = {
    var writer = df.write.mode(sink.sinkModel)
    writer
  }

  protected def buildPathWithDefaultSchema(uri: String, defaultUriSchema: String): String = {

    val path = uri.startsWith("/") match {
      case true => defaultUriSchema + uri
      case false => uri
    }

    path
  }

  def processImpl(df: Dataset[Row], defaultUriSchema: String): Unit = {
    val writer = fileWriter(df)
    var path = buildPathWithDefaultSchema(sink.sink_connection_url, defaultUriSchema)
    var format = sink.sinkFormat.name()
    format match {
      case "csv" => writer.option("delimiter", sink.csvSplit).mode(saveMode = SaveMode.Append).csv(path)
      case "json" =>
        val calendar = Calendar.getInstance()
        val format = new SimpleDateFormat("yyyy-MM-dd")
        calendar.add(Calendar.HOUR, -1)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        val timestamp = calendar.getTime.getTime
        val date = format.format(calendar.getTime)
        path += "/" + date + "/" + timestamp
        writer.mode(saveMode = SaveMode.Append).json(path)
      case "parquet" => writer.mode(saveMode = SaveMode.Append).parquet(path)
      case "text" => writer.text(path)
      case "orc" => writer.orc(path)
      case _ => writer.format(format).save(path)
    }
  }
}
