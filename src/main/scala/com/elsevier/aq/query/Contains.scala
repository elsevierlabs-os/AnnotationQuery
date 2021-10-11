package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to find annotations that contain another annotation.  The input is 2 Datasets of AQAnnotations.  We will call them A and B.  
 * The purpose is to find those annotations in A that contain B.  What that means is the start/end offset for an annotation from A  must contain the start/end offset from an annotation in  B.  
 * We of course have to also match on the document id.  We ultimately return the container annotations (A) that meet this criteria.  
 * We also deduplicate the A annotations as there could be many annotations from B that could be contained by an annotation in A but it only makes sense to return the unique container annotations.  
 * There is also the option of negating the query (think Not Contains) so that we return only A where it does not contain B. 
 */
class Contains(spark: SparkSession) {
  
  import spark.implicits._
  
  /**
   * @param left Dataset of AQAnnotations, the ones we will return if they contain AQAnnotations from 'right'.
   * @param right Dataset of AQAnnotations, the ones we are looking to see if they occur in the AQAnnotations from 'left'.
   * @param limit Number of AQAnnotations to return.
   * @param not Whether to negate the entire query (think NOT contains).  Default is false.
   * @return Dataset[AQAnnotation]
  */
  
  def apply(left: Dataset[AQAnnotation], right: Dataset[AQAnnotation], limit:Integer=0, not:Boolean=false): Dataset[AQAnnotation] = {
  
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
  
    if (not) {
      // No need to dedup as we should only have one row for each record on the left when the right is null
      results = left.as("L").join(right.as("R"),
                                  ($"L.docId" === $"R.docId" &&
                                   $"L.startOffset" <= $"R.startOffset" &&
                                   $"L.endOffset" >= $"R.endOffset" &&
                                   !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType"  && $"L.startOffset" === $"R.startOffset" && $"L.endOffset" === $"R.endOffset")),"leftouter")
                            .filter($"R.docId".isNull)
                            .select($"L.*")
                            .as[AQAnnotation]
    } else {
      // No need to dedup as leftsemi only returns unique rows from the left
      results = left.as("L").join(right.as("R"),
                                  ($"L.docId" === $"R.docId" &&
                                   $"L.startOffset" <= $"R.startOffset" &&
                                   $"L.endOffset" >= $"R.endOffset" &&
                                   !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType"  && $"L.startOffset" === $"R.startOffset" && $"L.endOffset" === $"R.endOffset")),"leftsemi")
                            .as[AQAnnotation]
    }

    if (limit > 0) {
      results.limit(limit)
    } else {
      results
    }
    
  }
              
}