package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to filter a property field using a regex expression in a Dataset of AQAnnotations.
 */
class RegexProperty(spark: SparkSession) {
  
  import spark.implicits._
  
  /**
   * @param ds Dataset of AQAnnotations that will be filtered by the specified property name and regex expression.
   * @param name Name of the property to filter.
   * @param regex Regex expression to use for the filter.
   * @param limit Number of AQAnnotations to return.
   * @param not Whether to negate the entire query. Default is false.
   * @return Dataset[AQAnnotation]
  */

  def apply(ds: Dataset[AQAnnotation], name:String, regex:String, limit:Integer=0, not:Boolean=false): Dataset[AQAnnotation] = {
  
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
  
    var query:String = "properties.`" + name + "` rlike " + "'" + regex + "'"
  
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