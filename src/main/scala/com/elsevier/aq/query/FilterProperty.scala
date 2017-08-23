package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to filter a property field with  a specified value in a Dataset of AQAnnotations. 
 * A single value or an array of values can be used for the filter comparison.
 */
object FilterProperty {
  
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  
  /*
   * ds - Dataset of AQAnnotations that will be filtered by the specified property name and value.
   * name - Name of the property to filter.
   * value - Value of the named property to filter.
   * valueArr - The array of values of the named property  to filter. An OR will be applied to the Strings. Only used if value was not specified.
   * valueCompare - Comparison operator to use for the property filter. Default is '='. Possible values are '=' and '!=' when valueArr specified. Possible values are '=','!=','<','<=','>', and '>=' otherwise.
   * limit - Number of AQAnnotations to return.
   * not - Whether to negate the entire query. Default is false.
  */

  def apply(ds: Dataset[AQAnnotation], name:String, value:String="", valueArr:Array[String]=Array.empty[String], valueCompare:String="=", limit:Integer=0, not:Boolean=false): Dataset[AQAnnotation] = {
  
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
  
    var query:String = ""
  
    if (value != "") {
      query += ("properties.`" + name + "` " + valueCompare + " '" + value  + "'")
    } else if (!valueArr.isEmpty) {
      if (valueCompare == "=") {
        query += ("properties.`" + name + "` in " + "('" + valueArr.mkString("','") + "')")
      } else {
        query += ("properties.`" + name + "` not in " + "('" + valueArr.mkString("','") + "')")
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