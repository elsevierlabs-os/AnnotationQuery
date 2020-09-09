package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to find annotations (looking at their property) that are in the same document. The input is 2 Datasets of AQAnnotations. We will call them A and B. 
 * The purpose is to find those annotations in A that are in the same document as B and also match values on the specified property.
 */
class MatchProperty(spark: SparkSession) {
  
  import spark.implicits._
  
  /*
   * left - Dataset of AQAnnotations, the ones we will return if they match AQAnnotations from 'right'.
   * right - Dataset of AQAnnotations the ones we are looking to see if they match AQAnnotations from 'left'.
   * name - Name of the property to match.
   * limit - Number of AQAnnotations to return.
   * not - Whether to negate the entire query (think NOT contains).  Default is false.
  */
 
  def apply(left: Dataset[AQAnnotation], 
            right: Dataset[AQAnnotation], 
            name:String,
            not:Boolean=false, 
            limit:Integer=0): Dataset[AQAnnotation] = {
  
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
  
    if (not) {
      results = left.as("L").join(right.as("R"),
                                  ($"L.docId" === $"R.docId"&&
                                   col("L.properties.`" + name + "`")  === col("R.properties.`" + name + "`")) ,"leftouter")
                            .filter($"R.docId".isNull)
                            .select($"L.*")
                            .as[AQAnnotation]    
    } else {    
      results = left.as("L").join(right.as("R"),
                                  ($"L.docId" === $"R.docId" &&
                                   col("L.properties.`" + name+ "`") === col("R.properties.`" + name+ "`")),"leftsemi")
                            .as[AQAnnotation]
    }

    if (limit > 0) {
      results.limit(limit)
    } else {
      results
    }
    
  }
              
}