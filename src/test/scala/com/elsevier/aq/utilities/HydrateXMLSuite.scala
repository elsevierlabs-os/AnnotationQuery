package com.elsevier.aq.utilities

import org.scalatest.FunSuite

import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.elsevier.aq.annotations.AQAnnotation
import com.elsevier.aq.annotations.CATAnnotation
import com.elsevier.aq.query.FilterType

class HydrateXMLSuite extends FunSuite {

  val conf = new SparkConf()
    .setAppName("GetAQAnnotationsSuite")
    .setMaster("local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  
  spark.conf.set("spark.sql.shuffle.partitions",2)

  import spark.implicits._
  
  val GetAQAnnotations = new GetAQAnnotations(spark)
  val FilterType = new FilterType(spark)
  val HydrateXML = new HydrateXML(spark)
  
  test("Check missing annotation file") {
      val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation],numPartitions=2)
      val sentenceAnnots = FilterType(aqAnnots,"sentence")
      val omAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/om/").as[CATAnnotation],Array("*"),decodeProps=Array("*"),numPartitions=2)
      val hydrateAnnots = HydrateXML(sentenceAnnots,"src/test/resources/junk/",omAnnots)
      assert(hydrateAnnots.collect()(0) == AQAnnotation("S0022314X13001777","ge","sentence",19912,20133,119,None))
  }

  test("Check sentence ... no xml") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation],numPartitions=2)
    val sentenceAnnots = FilterType(aqAnnots, "sentence")
    val omAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/om/").as[CATAnnotation],Array("*"),decodeProps=Array("*"),numPartitions=2)
    val hydrateAnnots = HydrateXML(sentenceAnnots, "./src/test/resources/str/",omAnnots)
    assert(hydrateAnnots.collect()(14) == AQAnnotation("S0022314X13001777","ge","sentence",23461,23469,870,Some(Map("xml" -> "(See cf."))))
  }

  test("Check sentence ... with xml") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation],numPartitions=2)
    val sentenceAnnots = FilterType(aqAnnots, "sentence")
    val omAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/om/").as[CATAnnotation],Array("*"),decodeProps=Array("*"),numPartitions=2)
    val hydrateAnnots = HydrateXML(sentenceAnnots, "./src/test/resources/str/",omAnnots)
    assert(hydrateAnnots.collect()(3) == AQAnnotation("S0022314X13001777","ge","sentence",20444,20489,241,Some(Map("xml" -> "Some notation: <ce:italic>p</ce:italic> is a fixed prime throughout."))))
  }  

}