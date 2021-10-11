package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to filter the annotation set field in a Dataset of AQAnnotations. 
 */
class FilterSet(spark: SparkSession) {
  
  import spark.implicits._
  
  /**
   * @param ds Dataset of AQAnnotations that will be filtered by the specified annotation set.
   * @param annotSet String to filter against the annotSet field in the dataset of AQAnnotations.
   * @param annotSetArr Array of Strings to filter against the annotSet field in the dataset of AQAnnotations. An OR will be applied to the Strings.  Only used if annotSet was not specified.
   * @param annotSetCompare Comparison operator to use for the annotSet field in the dataset of AQAnnotations.  Default is '='.  Possible values are '=' and '!='.
   * @param limit Number of AQAnnotations to return.
   * @param not Whether to negate the entire query.  Default is false.
   * @return Dataset[AQAnnotation]
  */

  def apply(ds: Dataset[AQAnnotation], annotSet:String="", annotSetArr:Array[String]=Array.empty[String], annotSetCompare:String="=", limit:Integer=0, not:Boolean=false): Dataset[AQAnnotation]  = {
  
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]

    var query:String = ""
  
    if (annotSet != "") {
      query += ("annotSet " + annotSetCompare + " \"" + annotSet + "\"")
    } else if (!annotSetArr.isEmpty) {
      if (annotSetCompare == "=") {
        query += ("annotSet in " + "('" + annotSetArr.mkString("','") + "')")
      } else {
        query += ("annotSet not in " + "('" + annotSetArr.mkString("','") + "')")
      }
    }

    if (not) {
      query = "!(" + query + ")"
    }
  
    results = ds.filter(query)

    if (limit > 0) {
      results.limit(limit)
    } else {
      results
    }
    
  }
  
}