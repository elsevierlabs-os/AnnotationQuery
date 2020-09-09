package com.elsevier.aq.query

import org.scalatest.FunSuite

import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.elsevier.aq.annotations.AQAnnotation
import com.elsevier.aq.annotations.CATAnnotation
import com.elsevier.aq.utilities.GetAQAnnotations

class QuerySuite extends FunSuite {

  val conf = new SparkConf()
    .setAppName("GetAQAnnotationsSuite")
    .setMaster("local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()
    
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  
  val GetAQAnnotations = new GetAQAnnotations(spark)
  val FilterProperty = new FilterProperty(spark)
  val RegexProperty = new RegexProperty(spark)
  val FilterSet = new FilterSet(spark)
  val FilterType = new FilterType(spark)
  val Contains = new Contains(spark)
  val ContainedIn = new ContainedIn(spark)
  val Before = new Before(spark)
  val After = new After(spark)
  val Between = new Between(spark)
  val Sequence = new Sequence(spark)
  val Or = new Or(spark)
  val And = new And(spark)
  val MatchProperty = new MatchProperty(spark)
  val Preceding = new Preceding(spark)
  val Following = new Following(spark)
  
  // Original Markup Annotations
  val omAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/om/").as[CATAnnotation], Array("orig"), Array("orig"), Array("orig"))

  // Genia Annotations
  val geniaAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/genia/").as[CATAnnotation], Array("orig", "lemma", "pos"), Array("lemma", "pos"), Array("orig", "lemma"))

  val annots = omAnnots.union(geniaAnnots)
                       .repartition(2,$"docId")
                       .sortWithinPartitions($"docId",$"startOffset",$"endOffset") 
                       
  spark.conf.set("spark.sql.shuffle.partitions",2)
  
  // Test FilterProperty

  test("FilterProperty(annots,'orig','and')") {
    assert(FilterProperty(annots, "orig", "and").count == 108)
  }

  test("FilterProperty(annots,'orig','and',limit=5)") {
    assert(FilterProperty(annots, "orig", "and", limit = 5).count == 5)
  }

  test("FilterProperty(annots,'orig','and',not=true)") {
    assert(FilterProperty(annots, "orig", "and", not = true).count == 3907)
  }

  test("FilterProperty(annots,'orig','and',valueCompare='!=')") {
    assert(FilterProperty(annots, "orig", "and", valueCompare = "!=").count == 3907)
  }

  test("FilterProperty(annots,'orig',valueArr=Array('and','to'))") {
    assert(FilterProperty(annots, "orig", valueArr = Array("and", "to")).count == 153)
  }

  // Test RegexProperty

  test("RegexProperty(annots,'orig','.*and.*')") {
    assert(RegexProperty(annots, "orig", ".*and.*").count == 135)
  }

  test("RegexProperty(annots,'orig','.*and.*',not=true)") {
    assert(RegexProperty(annots, "orig", ".*and.*", not = true).count == 3880)
  }

  // Test FilterSet 

  test("FilterSet(annots,'ge')") {
    assert(FilterSet(annots, "ge").count == 4066)
  }

  test("FilterSet(annots,'om')") {
    assert(FilterSet(annots, "om").count == 9754)
  }

  test("FilterSet(annots,Array('ge','om'))") {
    assert(FilterSet(annots, annotSetArr = Array("ge", "om")).count == 13820)
  }

  // Test FilterType

  test("FilterType(annots,'sentence')") {
    assert(FilterType(annots, "sentence").count == 128)
  }

  test("FilterType(annots,'ce:para')") {
    assert(FilterType(annots, "ce:para").count == 142)
  }

  test("FilterType(annots,Array('sentence','ce:para'))") {
    assert(FilterType(annots, annotTypeArr = Array("sentence", "ce:para")).count == 270)
  }

  // Test Contains

  test("Contains(FilterType(annots,'ce:para'),FilterType(annots,'sentence'))") {
    assert(Contains(FilterType(annots, "ce:para"), FilterType(annots, "sentence")).count == 138)
  }

  test("Contains(FilterType(annots,'ce:para'),FilterType(annots,'sentence'),not=true)") {
    assert(Contains(FilterType(annots, "ce:para"), FilterType(annots, "sentence"), not = true).count == 4)
  }

  // Test ContainedIn

  test("ContainedIn(FilterType(annots,'sentence'),FilterType(annots,'ce:para'))") {
    assert(ContainedIn(FilterType(annots, "sentence"), FilterType(annots, "ce:para")).count == 126)
  }

  test("ContainedIn(FilterType(annots,'sentence'),FilterType(annots,'ce:para'),not=true)") {
    assert(ContainedIn(FilterType(annots, "sentence"), FilterType(annots, "ce:para"), not = true).count == 2)
  }

  // Test Before

  test("Before(FilterProperty(annots,'orig','polynomial'),FilterProperty(annots,'orig','function'))") {
    assert(Before(FilterProperty(annots, "orig", "polynomial"), FilterProperty(annots, "orig", "function")).count == 47)
  }

  test("Before(FilterProperty(annots,'orig','polynomial'),FilterProperty(annots,'orig','function'),dist=50)") {
    assert(Before(FilterProperty(annots, "orig", "polynomial"), FilterProperty(annots, "orig", "function"), dist = 50).count == 11)
  }

  test("Before(FilterProperty(annots,'orig','polynomial'),FilterProperty(annots,'orig','function'),dist=50,not=true)") {
    assert(Before(FilterProperty(annots, "orig", "polynomial"), FilterProperty(annots, "orig", "function"), dist = 50, not = true).count == 36)
  }

  // Test After

  test("After(FilterProperty(annots,'orig','function'),FilterProperty(annots,'orig','polynomial'))") {
    assert(After(FilterProperty(annots, "orig", "function"), FilterProperty(annots, "orig", "polynomial")).count == 27)
  }

  test("After(FilterProperty(annots,'orig','function'),FilterProperty(annots,'orig','polynomial'),dist=50)") {
    assert(After(FilterProperty(annots, "orig", "function"), FilterProperty(annots, "orig", "polynomial"), dist = 50).count == 9)
  }

  test("After(FilterProperty(annots,'orig','function'),FilterProperty(annots,'orig','polynomial'),dist=50,not=true)") {
    assert(After(FilterProperty(annots, "orig", "function"), FilterProperty(annots, "orig", "polynomial"), dist = 50, not = true).count == 18)
  }

  // Test Between

  test("Between(FilterProperty(annots,'orig','permutations'),FilterProperty(annots,'orig','polynomial'),FilterProperty(annots,'orig','integers'))") {
    assert(Between(FilterProperty(annots, "orig", "permutations"), FilterProperty(annots, "orig", "polynomial"), FilterProperty(annots, "orig", "integers")).count == 2)
  }

  test("Between(FilterProperty(annots,'orig','permutations'),FilterProperty(annots,'orig','polynomial'),FilterProperty(annots,'orig','integers'),dist=50)") {
    assert(Between(FilterProperty(annots, "orig", "permutations"), FilterProperty(annots, "orig", "polynomial"), FilterProperty(annots, "orig", "integers"), dist = 50).count == 2)
  }

  test("Between(FilterProperty(annots,'orig','permutations'),FilterProperty(annots,'orig','polynomial'),FilterProperty(annots,'orig','integers'),dist=50,not=true)") {
    assert(Between(FilterProperty(annots, "orig", "permutations"), FilterProperty(annots, "orig", "polynomial"), FilterProperty(annots, "orig", "integers"), dist = 50, not = true).count == 16)
  }

  // Test Sequence

  test("Sequence(FilterProperty(annots,'orig','to'),FilterProperty(annots,'orig','be'),dist=5)") {
    assert(Sequence(FilterProperty(annots, "orig", "to"), FilterProperty(annots, "orig", "be"), dist = 5).count == 5)
  }

  // Test Or

  test("Or(omAnnots,geniaAnnots)") {
    assert(Or(omAnnots, geniaAnnots).count == 13820)
  }

  // Test And

  test("And(omAnnots,omAnnots)") {
    assert(And(omAnnots, omAnnots).count == 9754)
  }

  test("And(omAnnots,omAnnots,not=true)") {
    assert(And(omAnnots, omAnnots, not = true).count == 0)
  }

  test("And(omAnnots,geniaAnnots)") {
    assert(And(omAnnots, geniaAnnots).count == 9754)
  }

  test("And(omAnnots,geniaAnnots,not=true)") {
    assert(And(omAnnots, geniaAnnots, not = true).count == 0)
  }

  test("And(omAnnots,omAnnots,leftOnly=false)") {
    assert(And(omAnnots, omAnnots, leftOnly = false).count == 9754)
  }

  test("And(omAnnots,geniaAnnots,leftOnly=false)") {
    assert(And(omAnnots, geniaAnnots, leftOnly = false).count == 13820)
  }

  // Test MatchProperty

  test("MatchProperty(omAnnots,FilterType(omAnnots,'xocs:doi','orig')") {
    assert(MatchProperty(omAnnots, FilterType(omAnnots, "xocs:doi"), "orig").count == 1)
  }

  // Test Preceding
  
  test("Preceding(FilterType(annots,'sentence'),Contains(FilterType(annots,'sentence'),FilterProperty(annots,'orig','function')))") {
    val result = Preceding(FilterType(annots, "sentence"), Contains(FilterType(annots, "sentence"), FilterProperty(annots, "orig", "function")))
                 .collect()
                 .sortBy{ x => (x._1.startOffset, x._1.endOffset) }
    assert(result(0)._2.size == 2)
    assert(result(0)._1 == AQAnnotation("S0022314X13001777","ge","sentence",19649,19739,59,None))
    assert(result(0)._2(0) == AQAnnotation("S0022314X13001777","ge","sentence",19280,19471,14,None))
    assert(result(0)._2(1) == AQAnnotation("S0022314X13001777","ge","sentence",18546,18607,1,None))
  }
  
  test("Preceding(FilterType(annots,'sentence'),Contains(FilterType(annots,'sentence'),FilterProperty(annots,'orig','function')),FilterType(annots,'ce:para'))") {
    val result = Preceding(FilterType(annots, "sentence"), Contains(FilterType(annots, "sentence"), FilterProperty(annots, "orig", "function")),FilterType(annots,"ce:para"))
                 .collect()
                 .sortBy{ x => (x._1.startOffset, x._1.endOffset) }
    assert(result(0)._2.size == 0)
    assert(result(0)._1 == AQAnnotation("S0022314X13001777","ge","sentence",19649,19739,59,None))
  }
  
  // Test Following
  
  test("Following(FilterType(annots,'sentence'),Contains(FilterType(annots,'sentence'),FilterProperty(annots,'orig','function')),cnt=20)") {
    val result = Following(FilterType(annots, "sentence"), Contains(FilterType(annots, "sentence"), FilterProperty(annots, "orig", "function")),cnt=20)
                 .collect()
                 .sortBy{ x => (x._1.startOffset, x._1.endOffset) }
    assert(result(0)._2.size == 20)
    assert(result(0)._1 == AQAnnotation("S0022314X13001777","ge","sentence",19649,19739,59,None))
    assert(result(0)._2(0) == AQAnnotation("S0022314X13001777","ge","sentence",19740,19911,80,None))
    assert(result(0)._2(1) == AQAnnotation("S0022314X13001777","ge","sentence",19912,20133,119,None))
    assert(result(0)._2(2) == AQAnnotation("S0022314X13001777","ge","sentence",20134,20311,167,None))
  }
  
  test("Following(FilterType(annots,'sentence'),Contains(FilterType(annots,'sentence'),FilterProperty(annots,'orig','function')),FilterType(annots,'ce:para')20)") {
    val result = Following(FilterType(annots, "sentence"), Contains(FilterType(annots, "sentence"), FilterProperty(annots, "orig", "function")),FilterType(annots,"ce:para"),20)
                 .collect()
                 .sortBy{ x => (x._1.startOffset, x._1.endOffset) }
    assert(result(0)._2.size == 3)
    assert(result(0)._1 == AQAnnotation("S0022314X13001777","ge","sentence",19649,19739,59,None))
    assert(result(0)._2(0) == AQAnnotation("S0022314X13001777","ge","sentence",19740,19911,80,None))
    assert(result(0)._2(1) == AQAnnotation("S0022314X13001777","ge","sentence",19912,20133,119,None))
    assert(result(0)._2(2) == AQAnnotation("S0022314X13001777","ge","sentence",20134,20311,167,None))
  } 

  // NPE temp fix
  
  test("Preceding(FilterType(annots,'none'),Contains(FilterType(annots,'sentence'),FilterProperty(annots,'orig','function')),FilterType(annots,'ce:para'))") {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    val result = Preceding(FilterType(annots, "none"), Contains(FilterType(annots, "sentence"), FilterProperty(annots, "orig", "function")),FilterType(annots,"ce:para"))
                 .collect()
                 .sortBy{ x => (x._1.startOffset, x._1.endOffset) }
    assert(result.size == 21)
    assert(result(0)._1 == AQAnnotation("S0022314X13001777","ge","sentence",19649,19739,59,None))
    assert(result(0)._2.isEmpty == true)
  }  

  test("Following(FilterType(annots,'none'),Contains(FilterType(annots,'sentence'),FilterProperty(annots,'orig','function')),FilterType(annots,'ce:para'),20)") {
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    val result = Following(FilterType(annots, "none"), Contains(FilterType(annots, "sentence"), FilterProperty(annots, "orig", "function")),FilterType(annots,"ce:para"),20)
                 .collect()
                 .sortBy{ x => (x._1.startOffset, x._1.endOffset) }    
    assert(result.size == 21)
    assert(result(0)._1 == AQAnnotation("S0022314X13001777","ge","sentence",19649,19739,59,None))
    assert(result(0)._2.isEmpty == true)
  }

  
}