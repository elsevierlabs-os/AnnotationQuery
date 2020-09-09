package com.elsevier.aq.utilities

import org.scalatest.FunSuite

import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.elsevier.aq.annotations.AQAnnotation
import com.elsevier.aq.annotations.CATAnnotation

class GetCATAnnotationsSuite extends FunSuite {

  val conf = new SparkConf()
    .setAppName("GetCATAnnotationsSuite")
    .setMaster("local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  
  val GetAQAnnotations = new GetAQAnnotations(spark)
  val GetCATAnnotations = new GetCATAnnotations(spark)
  
  test("Check count of annotations") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos", "excludes"), Array("lemma", "pos"), Array("orig", "lemma"))
    val catAnnots: Dataset[CATAnnotation] = GetCATAnnotations(aqAnnots, Array("orig", "lemma", "pos"), Array("orig", "lemma"))
    assert(catAnnots.count == 4066)
  }

  test("Check an annotation") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos", "excludes"), Array("lemma", "pos"), Array("orig", "lemma"))
    val catAnnots: Dataset[CATAnnotation] = GetCATAnnotations(aqAnnots, Array("orig", "lemma", "pos"), Array("orig", "lemma")).sort("docId", "startOffset", "endOffset")
    assert(catAnnots.collect()(3) == CATAnnotation("S0022314X13001777","ge","word",18552,18560,4,Some("orig=p-groups&lemma=p-group&pos=nns")))
  }
  
  test("Check property wildcard") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos", "excludes"), Array("lemma", "pos"), Array("orig", "lemma"))
    val catAnnots: Dataset[CATAnnotation] = GetCATAnnotations(aqAnnots, Array("*")).sort("docId", "startOffset", "endOffset")
    assert(catAnnots.collect()(3) == CATAnnotation("S0022314X13001777","ge","word",18552,18560,4,Some("orig=p-groups&lemma=p-group&pos=nns")))
  }
  
  test("Check encode wildcard") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos", "excludes"), Array("lemma", "pos"), Array("orig", "lemma"))
    val catAnnots: Dataset[CATAnnotation] = GetCATAnnotations(aqAnnots, Array("*"),Array("*")).sort("docId", "startOffset", "endOffset")
    assert(catAnnots.collect()(3) == CATAnnotation("S0022314X13001777","ge","word",18552,18560,4,Some("orig=p-groups&lemma=p-group&pos=nns")))
  }

}