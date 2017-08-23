package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to find annotations that are contained by another annotation.  The input is 2 Datasets of AQAnnotations.  We will call them A and B.  
 * The purpose is to find those annotations in A that are contained in B.  What that means is the start/end offset for an annotation from A  must be contained by the start/end offset from an annotation in  B.  
 * We of course have to also match on the document id.  We ultimately return the contained annotations (A) that meet this criteria.  
 * There is also the option of negating the query (think Not Contains) so that we return only A where it is not contained in B. 
 */
object ContainedIn {
  
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  
  /*
   * left - Dataset of AQAnnotations, the ones we will return if they are contained in AQAnnotations from 'right'.
   * right - Dataset of AQAnnotations, the ones we are looking to see if they contain AQAnnotations from 'left'.
   * limit - Number of AQAnnotations to return.
   * not - Whether to negate the entire query (think NOT contained in).  Default is false.
  */
  
  def apply(left: Dataset[AQAnnotation], right: Dataset[AQAnnotation], limit:Integer=0, not:Boolean=false): Dataset[AQAnnotation] = {
  
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
  
    if (not) {
      // No need to dedup as we should only have one row for each record on the left when the right is null
      results = left.as("L").join(right.as("R"),
                                  ($"L.docId" === $"R.docId" &&
                                   $"L.startOffset" >= $"R.startOffset" &&
                                   $"L.endOffset" <= $"R.endOffset" &&
                                   !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType" && $"L.annotId" === $"R.annotId")), "leftouter")
                            .filter($"R.docId".isNull)
                            .select($"L.*")
                            .as[AQAnnotation]           
    } else {
      // No need to dedup as leftsemi only returns unique rows from the left
      results = left.as("L").join(right.as("R"),
                                  ($"L.docId" === $"R.docId" &&
                                   $"L.startOffset" >= $"R.startOffset" &&
                                   $"L.endOffset" <= $"R.endOffset" &&
                                   !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType" && $"L.annotId" === $"R.annotId")),"leftsemi")
                            .as[AQAnnotation]                  
    }
  
    if (limit > 0) {
      results.limit(limit)
    } else {
      results
    }
    
  }
              
}