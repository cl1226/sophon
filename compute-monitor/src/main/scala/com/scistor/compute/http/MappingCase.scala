package com.scistor.compute.http

import com.scistor.compute.annotation.Mappings

import scala.util.matching.Regex

case class MappingCase(method: String, mappings: Mappings, pattern: Regex)