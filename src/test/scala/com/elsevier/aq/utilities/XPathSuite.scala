package com.elsevier.aq.utilities

import org.scalatest.FunSuite

import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.elsevier.aq.annotations.AQAnnotation
import com.elsevier.aq.annotations.CATAnnotation
import com.elsevier.aq.query.FilterType

class XPathSuite extends FunSuite {

  val conf = new SparkConf()
    .setAppName("GetXPathSuite")
    .setMaster("local[2]")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  
  spark.conf.set("spark.sql.shuffle.partitions",2)

  import spark.implicits._
  
  val GetAQAnnotations = new GetAQAnnotations(spark)
  val FilterType = new FilterType(spark)
  val XPath = new XPath(spark)
  
  test("Check with found om annotation file") {
      val omAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/om/").as[CATAnnotation],Array("*"),decodeProps=Array("*"),numPartitions=2)
      val xpathAnnots = XPath(omAnnots,"src/test/resources/omfiles/")
      assert(9754 == xpathAnnots.count)
      val annots = xpathAnnots.limit(100).collect()
      assert(annots(22) == AQAnnotation("S0022314X13001777","om","xocs:timestamp",232,264,23,Some(Map("xpath" -> "/xocs:doc[1]/xocs:meta[1]/xocs:timestamp[1]", "attr" -> "yyyymmdd=20140927", "orig" -> "2014-09-27T22:27:15.258392-04:00", "parentId" -> "2", "origAnnotID" -> "23"))))
      assert(annots(38) == AQAnnotation("S0022314X13001777","om","xocs:volume",1094,1097,39,Some(Map("parentId" -> "38", "orig" -> "133", "origAnnotID" -> "39", "xpath" -> "/xocs:doc[1]/xocs:meta[1]/xocs:volume-list[1]/xocs:volume[1]"))))
      assert(annots(99) == AQAnnotation("S0022314X13001777","om","xocs:item-toc-section-title",1465,1475,100,Some(Map("parentId" -> "99", "orig" -> "References", "origAnnotID" -> "100", "xpath" -> "/xocs:doc[1]/xocs:meta[1]/xocs:item-toc[1]/xocs:item-toc-entry[7]/xocs:item-toc-section-title[1]"))))
  }
  
  test("Check with missing om annotation file") {
      val omAnnots: Dataset[AQAnnotation] = GetAQAnnotations(spark.read.parquet("./src/test/resources/om/").as[CATAnnotation],Array("*"),decodeProps=Array("*"),numPartitions=2)
      val xpathAnnots = XPath(omAnnots,"src/test/resources/junk/")
      assert(9754 == xpathAnnots.count)
      val annots = xpathAnnots.limit(100).collect()
      assert(annots(48) == AQAnnotation("S0022314X13001777","om","xocs:last-page",1135,1139,49,Some(Map("parentId" -> "47", "orig" -> "4199", "origAnnotID" -> "49", "xpath" -> "ERR"))))
  }
 
}