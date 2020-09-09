package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation

/**
 * Provide the ability to find annotations that are before another annotation. The input is 2 Datasets of AQAnnotations. We will call them A and B. 
 * The purpose is to find those annotations in A that are before B. What that means is the end offset for an annotation from A must be before the start offset from an annotation in B. 
 * We of course have to also match on the document id. We ultimately return the A annotations that meet this criteria.  
 * A distance operator can also be optionally specified.  This would require an A annotation (endOffset) to occur n characters (or less) before the B annotation (startOffset). 
 * There is also the option of negating the query (think Not Before) so that we return only A where it is not before B. 
 */
class Before(spark: SparkSession) {
   
  import spark.implicits._
  
  /*
   * left - Dataset of AQAnnotations, the ones we will return if they are before AQAnnotations from 'right'.
   * right - Dataset of AQAnnotations, the ones we are looking to see if are after AQAnnotations from 'left'.
   * dist  - Number of characters  where endOffset from 'left' must occur before startOffset from 'right'. Default is Int.MaxValue.
   * limit - Number of AQAnnotations to return.
   * not - Whether to negate the entire query (think NOT before).  Default is false.
  */

  def apply(left: Dataset[AQAnnotation], right: Dataset[AQAnnotation], dist:Int=Int.MaxValue, limit:Integer=0, not:Boolean=false): Dataset[AQAnnotation] = {

    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
  
    if (not) {
      // No need to dedup as we should only have one row for each record on the left when the right is null
      results = left.as("L").join(right.as("R"),
                                  ($"L.docId" === $"R.docId" &&
                                   $"R.startOffset" >=  $"L.endOffset" && 
                                   $"R.startOffset" - $"L.endOffset" < dist &&
                                   !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType"  && $"L.startOffset" === $"R.startOffset" && $"L.endOffset" === $"R.endOffset")),"leftouter")
                            .filter($"R.docId".isNull)
                            .select($"L.*")
                            .as[AQAnnotation]  
    } else {
      // No need to dedup as leftsemi only returns unique rows from the left
      results = left.as("L").join(right.as("R"),
                                  ($"L.docId" === $"R.docId" &&
                                   $"R.startOffset" >=  $"L.endOffset" && 
                                   $"R.startOffset" - $"L.endOffset" < dist &&
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