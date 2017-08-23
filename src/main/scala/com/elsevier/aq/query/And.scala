package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to find annotations that are in the same document. The input is 2 Datasets of AQAnnotations. We will call them A and B. 
 * The purpose is to find those annotations in A and B that are in the same document.  
 */
object And {
   
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  
  /*
   * left - Dataset of AQAnnotations
   * right - Dataset of AQAnnotations.
   * limit - Number of AQAnnotations to return.
   * not - think and NOT (only return annotations from A that are not in B).  Default is false.
   * leftOnly - Reuturn only the left or the left and right.  The default is to only return the left.
  */
  
  def apply(left: Dataset[AQAnnotation], right: Dataset[AQAnnotation], limit:Integer=0, not:Boolean=false, leftOnly:Boolean=true): Dataset[AQAnnotation] = {
  
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
  
    if (not) {
       results = left.as("L").join(right.select($"docId").distinct.as("R"),
                                   ($"L.docId" === $"R.docId"),"leftouter")
                             .filter($"R.docId".isNull)
                             .select($"L.*")
                             .as[AQAnnotation]   
    } else {
      if (leftOnly) {
        results = left.as("L").join(right.select($"docId").distinct.as("R"),
                                    ($"L.docId" === $"R.docId"),"leftsemi")
                              .as[AQAnnotation]

      } else {
        // Still very slow.  Essentially a Cartesian join removing duplicates
        results = left.as("L").joinWith(right.as("R"),
                                        ($"L.docId" === $"R.docId"))
                              .flatMap(rec => {
                                       List(rec._1,rec._2)
                              })
                              .as[AQAnnotation]
                              // Should really take into account the Map of Properties
                              .dropDuplicates("docId","annotSet","annotType","annotId","startOffset","endOffset")
      }
    }
  
    if (limit > 0) {
      results.limit(limit)
    } else {
      results
    }
    
  }
  
}