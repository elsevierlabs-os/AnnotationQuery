package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to combine (union) annotations. The input is 2 Datasets of AQAnnotations. The output is the union of these annotations.
 */
class Or(spark: SparkSession) {
    
  import spark.implicits._
  
  /*
   * left - Dataset of AQAnnotations
   * right - Dataset of AQAnnotations
   * limit - Number of AQAnnotations to return.
  */

  def apply(left: Dataset[AQAnnotation], right: Dataset[AQAnnotation], limit:Integer=0): Dataset[AQAnnotation] = {
  
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
  
    results = left.as("L").joinWith(right.as("R"),
                                    ($"L.docId" === $"R.docId" &&
                                     $"L.annotSet" === $"R.annotSet" &&
                                     $"L.annotType" === $"R.annotType" &&
                                     $"L.startOffset" === $"R.startOffset" &&
                                     $"L.endOffset" === $"R.endOffset" &&
                                   $"L.annotId" === $"R.annotId"),"outer")
                          .map(rec => if (rec._1 == null) rec._2
                                      else if (rec._2 == null) rec._1
                                      else rec._1)
                          .as[AQAnnotation]
                       
  
    if (limit > 0) {
      results.limit(limit)
    } else {
      results
    }
    
  }
  
}