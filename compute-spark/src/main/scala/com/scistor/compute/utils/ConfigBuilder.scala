package com.scistor.compute.utils

import java.util.ServiceLoader

import com.scistor.compute.apis.{BaseOutput, BaseStaticInput, BaseStreamingInput, BaseTransform, Plugin}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.util.control.Breaks.{break, breakable}

class ConfigBuilder {

  def createStaticInputs[T <: Plugin](engine: String): List[BaseStaticInput] = {

    var inputList = List[BaseStaticInput]()
    JobInfoTransfer.staticInputs.foreach(input => {
      val className = buildClassFullQualifier(input._2.sourceType.name(), "input", engine)

      val obj = Class
        .forName(className)
        .newInstance()
        .asInstanceOf[T]

      obj match {
        case inputObject: BaseStaticInput => {
          val baseStaticInput = inputObject.asInstanceOf[BaseStaticInput]
          baseStaticInput.setSource(input._2)
          inputList = inputList :+ baseStaticInput
        }
        case _ => // do nothing
      }

    })

    inputList
  }

  def createStreamingInputs(engine: String): List[BaseStreamingInput[Any]] = {

    var inputList = List[BaseStreamingInput[Any]]()
    JobInfoTransfer.streamingInputs.foreach(input => {
      val className = buildClassFullQualifier(s"${input._2.sourceType.name()}Stream", "input", engine)

      val obj = Class
        .forName(className)
        .newInstance()

      obj match {
        case inputObject: BaseStreamingInput[Any] => {
          val baseStreamingInput = inputObject.asInstanceOf[BaseStreamingInput[Any]]
          baseStreamingInput.setSource(input._2)
          inputList = inputList :+ baseStreamingInput
        }
        case _ => // do nothing
      }
    })

    inputList
  }

  def createTransforms[T <: Plugin](engine: String): Map[String, BaseTransform] = {

    var transformMap = Map[String, BaseTransform]()
    JobInfoTransfer.transforms.foreach(transform => {
      val className = buildClassFullQualifier(transform._2.getPluginName, "transform", engine)

      val obj = Class
        .forName(className)
        .newInstance()
        .asInstanceOf[T]

      obj match {
        case transformObject: BaseTransform => {
          val baseTransform: BaseTransform = transformObject.asInstanceOf[BaseTransform]
          baseTransform.setAttribute(transform._2)
          transformMap += (transform._1 -> baseTransform)
        }
        case _ => // do nothing
      }

    })

    transformMap
  }

  def createOutputs[T <: Plugin](engine: String): List[BaseOutput] = {

    var outputList = List[BaseOutput]()
    JobInfoTransfer.outputs.foreach(output => {
      val className = engine match {
        case "batch" | "sparkstreaming" =>
          buildClassFullQualifier(output._2.sinkType.name(), "output", "batch")
        case "structuredstreaming" =>
          buildClassFullQualifier(output._2.sinkType.name(), "output", engine)
      }

      val obj = Class
        .forName(className)
        .newInstance()
        .asInstanceOf[T]

      obj match {
        case outputObject: BaseOutput => {
          val baseOutput: BaseOutput = outputObject.asInstanceOf[BaseOutput]
          baseOutput.setSink(output._2)
          outputList = outputList :+ baseOutput
        }
        case _ => // do nothing
      }

    })

    outputList
  }

  private def getInputType(name: String, engine: String): String = {
    name match {
      case _ if name.toLowerCase.endsWith("stream") => {
        engine match {
          case "batch" => "sparkstreaming"
          case "structuredstreaming" => "structuredstreaming"
        }
      }
      case _ => "batch"
    }
  }

  private def buildClassFullQualifier(name: String, classType: String, engine: String): String = {

    var qualifier = name.substring(0, 1).concat(name.substring(1).toLowerCase()).replace("stream", "Stream")
    if (qualifier.split("\\.").length == 1) {

      val packageName = classType match {
        case "input" => ConfigBuilder.InputPackage + "." + getInputType(name, engine)
        case "transform" => ConfigBuilder.TransformPackage
        case "output" => ConfigBuilder.OutputPackage + "." + engine
      }

      val services: Iterable[Plugin] =
        (ServiceLoader load classOf[BaseStaticInput]).asScala ++
          (ServiceLoader load classOf[BaseOutput]).asScala ++
          (ServiceLoader load classOf[BaseStreamingInput[Any]]).asScala

      var classFound = false
      breakable {
        for (serviceInstance <- services) {
          val clz = serviceInstance.getClass
          // get class name prefixed by package name
          val clzNameLowercase = clz.getName.toLowerCase()
          val qualifierWithPackage = packageName + "." + qualifier
          if (clzNameLowercase == qualifierWithPackage.toLowerCase) {
            qualifier = clz.getName
            classFound = true
            break
          }
        }
      }
    }

    qualifier
  }
}

object ConfigBuilder {
  val PackagePrefix = "com.scistor.compute"
  val TransformPackage = PackagePrefix + ".transform"
  val InputPackage = PackagePrefix + ".input"
  val OutputPackage = PackagePrefix + ".output"

  val SourceType = "sourceType"
}
