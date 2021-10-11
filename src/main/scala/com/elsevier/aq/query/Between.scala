package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Provide the ability to find annotations that are before one annotation and after another. The input is 3 Datasets of AQAnnotations. We will call them A, B and C. 
 * The purpose is to find those annotations in A that are before B and after C. 
 * What that means is the end offset for an annotation from A must be before the start offset from an annotation in B and the start offset for A be after the end offset from C. 
 * We of course have to also match on the document id. We ultimately return the A annotations that meet this criteria. 
 * A distance operator can also be optionally specified. This would require an A annotation (endOffset) to occur n characters (or less) before the B annotation (startOffset) and would require the A annotation (startOffset) to occur n characters (or less) after the C annotation (endOffset) . 
 * There is also the option of negating the query (think Not Between) so that we return only A where it is not before B nor after C.
 */
class Between(spark: SparkSession) {
    
  import spark.implicits._
  
  /**
   * @param middle Dataset of AQAnnotations, the ones we will return if they are between AQAnnotations from 'left' and AQAnnotations from 'right.
   * @param left Dataset of AQAnnotations, the ones we are looking to see if they are before AQAnnotations from 'middle'.
   * @param right Dataset of AQAnnotations, the ones we are looking to see if they are after AQAnnotations from 'middle'.
   * @param dist  Number of characters  where startOffset from 'middle' must occur after endOffset of 'left' or endOffset from 'middle' must occur before startOffset of 'right'
   * @param limit Number of AQAnnotations to return.
   * @param not Whether to negate the entire query (think NOT between).  Default is false.
   * @return Dataset[AQAnnotation]
  */

  def apply(middle: Dataset[AQAnnotation], left: Dataset[AQAnnotation], right: Dataset[AQAnnotation], dist:Int=Int.MaxValue, limit:Integer=0, not:Boolean=false): Dataset[AQAnnotation] = {

    var intermediate:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
    var intermediate2:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
  
    if (not) {
      // No need to dedup as leftsemi only returns unique rows from the left
      intermediate = middle.as("L").join(right.as("R"),
                                    ($"L.docId" === $"R.docId" &&
                                     $"R.startOffset" >=  $"L.endOffset" && 
                                     $"R.startOffset" - $"L.endOffset" < dist &&
                                     !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType"  && $"L.startOffset" === $"R.startOffset" && $"L.endOffset" === $"R.endOffset")),"leftsemi")
                                   .as[AQAnnotation]
    
      intermediate2 = intermediate.as("L").join(left.as("R"),
                                           ($"L.docId" === $"R.docId" &&
                                            $"L.startOffset" >=  $"R.endOffset" && 
                                            $"L.startOffset" - $"R.endOffset" < dist &&
                                            !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType"  && $"L.startOffset" === $"R.startOffset" && $"L.endOffset" === $"R.endOffset")),"leftsemi")
                                          .as[AQAnnotation]  
    
      results = middle.as("L").join(intermediate2.as("R"),
                                    ($"L.docId" === $"R.docId" &&
                                     $"L.annotSet" ===  $"R.annotSet" &&
                                     $"L.annotType" === $"R.annotType" &&
                                     $"L.annotId" === $"R.annotId"), "leftouter")
                              .filter($"R.docId".isNull)
                              .select($"L.*")
                              .as[AQAnnotation]
    
    } else {
      // No need to dedup as leftsemi only returns unique rows from the left
      intermediate = middle.as("L").join(right.as("R"),
                                        ($"L.docId" === $"R.docId" &&
                                         $"R.startOffset" >=  $"L.endOffset" && 
                                         $"R.startOffset" - $"L.endOffset" < dist &&
                                         !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType"  && $"L.startOffset" === $"R.startOffset" && $"L.endOffset" === $"R.endOffset")),"leftsemi")
                                   .as[AQAnnotation]   
      results = intermediate.as("L").join(left.as("R"),
                                         ($"L.docId" === $"R.docId" &&
                                          $"L.startOffset" >=  $"R.endOffset" && 
                                          $"L.startOffset" - $"R.endOffset" < dist &&
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