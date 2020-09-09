package com.elsevier.aq.utilities

import org.scalatest.FunSuite

import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.elsevier.aq.annotations.AQAnnotation
import com.elsevier.aq.annotations.CATAnnotation

class GetAQAnnotationsSuite extends FunSuite {

  val conf = new SparkConf()
    .setAppName("GetAQAnnotationsSuite")
    .setMaster("local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  
  import spark.implicits._
  
  val GetAQAnnotations = new GetAQAnnotations(spark)

  test("Check count of annotations") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos", "excludes"), Array("lemma", "pos"), Array("orig", "lemma"))
    assert(aqAnnots.count == 4066)
  }

  test("Check an annotation") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos", "excludes"), Array("lemma", "pos"), Array("orig", "lemma")).sort("docId", "startOffset", "endOffset", "annotType")
    assert(aqAnnots.take(1)(0) == AQAnnotation("S0022314X13001777","ge","word",18546,18551,3,Some(Map("orig" -> "Sylow", "lemma" -> "sylow", "pos" -> "jj"))))
  }
  
  test("Check property wildcard") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("*")).sort("docId", "startOffset", "endOffset", "annotType")
    assert(aqAnnots.take(1)(0) == AQAnnotation("S0022314X13001777","ge","word",18546,18551,3,Some(Map("lemma" -> "sylow", "pos" -> "JJ", "tokidx" -> "1", "orig" -> "Sylow", "parentId" -> "4054", "origAnnotID" -> "4055"))))
  }

  test("Check lower case wildcard") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("*"),Array("*")).sort("docId", "startOffset", "endOffset", "annotType")
    assert(aqAnnots.take(1)(0) == AQAnnotation("S0022314X13001777","ge","word",18546,18551,3,Some(Map("lemma" -> "sylow", "pos" -> "jj", "tokidx" -> "1", "orig" -> "sylow", "parentId" -> "4054", "origAnnotID" -> "4055"))))
  }

  test("Check url decode wildcard") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("*"),Array.empty,Array("*")).sort("docId", "startOffset", "endOffset", "annotType")
    assert(aqAnnots.take(1)(0) == AQAnnotation("S0022314X13001777","ge","word",18546,18551,3,Some(Map("lemma" -> "sylow", "pos" -> "JJ", "tokidx" -> "1", "orig" -> "Sylow", "parentId" -> "4054", "origAnnotID" -> "4055"))))
  }
    
}
