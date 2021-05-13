package com.elsevier.aq.utilities

import org.scalatest.FunSuite

import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.elsevier.aq.annotations.AQAnnotation
import com.elsevier.aq.annotations.CATAnnotation
import com.elsevier.aq.query.FilterType

class HydrateSuite extends FunSuite {

  val conf = new SparkConf()
    .setAppName("GetHydrateSuite")
    .setMaster("local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  
  val GetAQAnnotations = new GetAQAnnotations(spark)
  val FilterType = new FilterType(spark)
  val Hydrate = new Hydrate(spark)
  
  test("Check missing annotation file") {
      val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation],Array("orig","lemma","pos","excludes"),Array("lemma","pos"),Array("orig","lemma"))
      val sentenceAnnots = FilterType(aqAnnots,"sentence")
      val hydrateAnnots = Hydrate(sentenceAnnots,"src/test/resources/junk/")
      assert(hydrateAnnots.collect()(0) == AQAnnotation("S0022314X13001777","ge","sentence",18546,18607,1,None))
  }
  
  test("Check sentence") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos", "excludes"), Array("lemma", "pos"), Array("orig", "lemma"))
    val sentenceAnnots = FilterType(aqAnnots, "sentence")
    val hydrateAnnots = Hydrate(sentenceAnnots, "./src/test/resources/str/")
    assert(hydrateAnnots.collect()(0) == AQAnnotation("S0022314X13001777","ge","sentence",18546,18607,1,Some(Map("text" -> "Sylow p-groups of polynomial permutations on the integers mod"))))
    }

  test("Check sentence with excludes") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos", "excludes"), Array("lemma", "pos"), Array("orig", "lemma"))
    val sentenceAnnots = FilterType(aqAnnots, "sentence")
    val hydrateAnnots = Hydrate(sentenceAnnots, "./src/test/resources/str/")
    assert(hydrateAnnots.collect()(8) == AQAnnotation("S0022314X13001777","ge","sentence",20490,20777,256,Some(Map("excludes" -> "2872,om,mml:math,20501,20510|2894,om,mml:math,20540,20546|2907,om,mml:math,20586,20590|2913,om,mml:math,20627,20630|2923,om,mml:math,20645,20651|2933,om,mml:math,20718,20721", "text" -> "A function  arising from a polynomial in  or, equivalently, from a polynomial in , is called a polynomial function on . We denote by  the monoid with respect to composition of polynomial functions on . By monoid, we mean semigroup with an identity element."))))
    }

  test("Check sentence without excludes") {
    val aqAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos", "excludes"), Array("lemma", "pos"), Array("orig", "lemma"))
    val sentenceAnnots = FilterType(aqAnnots, "sentence")
    val hydrateAnnots = Hydrate(sentenceAnnots, "./src/test/resources/str/", false)
    assert(hydrateAnnots.collect()(8) == AQAnnotation("S0022314X13001777","ge","sentence",20490,20777,256,Some(Map("excludes" -> "2872,om,mml:math,20501,20510|2894,om,mml:math,20540,20546|2907,om,mml:math,20586,20590|2913,om,mml:math,20627,20630|2923,om,mml:math,20645,20651|2933,om,mml:math,20718,20721", "text" -> "A function g:Zpn→Zpn arising from a polynomial in Zpn[x] or, equivalently, from a polynomial in Z[x], is called a polynomial function on Zpn. We denote by (Fn,∘) the monoid with respect to composition of polynomial functions on Zpn. By monoid, we mean semigroup with an identity element."))))
    }

}