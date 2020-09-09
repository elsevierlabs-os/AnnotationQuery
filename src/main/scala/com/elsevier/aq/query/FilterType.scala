package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to filter the annotation type field in a Dataset of AQAnnotations. 
 */
class FilterType(spark: SparkSession) {
  
  import spark.implicits._
  
  /*
   * ds - Dataset of AQAnnotations that will be filtered by the specified annotation type.
   * annotType - String to filter against the annotType field in the dataset of AQAnnotations.
   * annotTypeArr - Array of Strings to filter against the annotType field in the dataset of AQAnnotations. An OR will be applied to the Strings.  Only used if annotType was not specified.
   * annotTypeCompare - Comparison operator to use for the annotType field in the dataset of AQAnnotations.  Default is '='.  Possible values are '=' and '!='.
   * limit - Number of AQAnnotations to return.
   * not - Whether to negate the entire query.  Default is false.
  */

  def apply(ds: Dataset[AQAnnotation], annotType:String="", annotTypeArr:Array[String]=Array.empty[String], annotTypeCompare:String="=", limit:Integer=0, not:Boolean=false): Dataset[AQAnnotation]  = {
  
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]

    var query:String = ""
  
    if (annotType != "") {
      query += ("annotType " + annotTypeCompare + " \"" + annotType + "\"")
    } else if (!annotTypeArr.isEmpty) {
      if (annotTypeCompare == "=") {
        query += ("annotType in " + "('" + annotTypeArr.mkString("','") + "')")
      } else {
        query += ("annotType not in " + "('" + annotTypeArr.mkString("','") + "')")
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