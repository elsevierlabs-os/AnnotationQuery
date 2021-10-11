package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
import org.apache.spark.sql.functions._

/**
 * Provide the ability to find annotations that are before another annotation. The input is 2 Datasets of AQAnnotations. We will call them A and B.
 * The purpose is to find those annotations in A that are before B. What that means is the end offset for an annotation from A must be before the start offset from an annotation in B.
 * We of course have to also match on the document id. We ultimately return the annotations that meet this criteria. Unlike the Before function, we adjust the returned annotation a bit.
 * For example, we set the annotType to "seq" and we use the A startOffset and the B endOffset.
 * A distance operator can also be optionally specified.  This would require an A annotation (endOffset) to occur n characters (or less) before the B annotation (startOffset).
 */
class Sequence(spark: SparkSession) {

  import spark.implicits._

  /**
   * @param left Dataset of AQAnnotations, the ones we will return if they are before AQAnnotations from 'right'.
   * @param right Dataset of AQAnnotations, the ones we are looking to see if are after AQAnnotations from 'left'.
   * @param dist  Number of characters  where endOffset from 'left' must occur before startOffset from 'right'. Default is Int.MaxValue.
   * @param limit Number of AQAnnotations to return.
   * @return Dataset[AQAnnotation]
  */

  def apply(left: Dataset[AQAnnotation], right: Dataset[AQAnnotation], dist:Int=Int.MaxValue, limit:Integer=0): Dataset[AQAnnotation] = {

    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]

    results = left.as("L").join(right.as("R"),
                                ($"L.docId" === $"R.docId" &&
                                 $"R.startOffset" >=  $"L.endOffset" &&
                                 $"R.startOffset" - $"L.endOffset" < dist &&
                                 !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType"  && $"L.startOffset" === $"R.startOffset" && $"L.endOffset" === $"R.endOffset")))
                          .select($"L.docId", $"L.annotSet", $"L.annotId", $"L.startOffset", $"R.endOffset")
                          .withColumn("annotType",lit("seq"))
                          .withColumn("properties",typedLit(Map[String, String]()))
                          .as[AQAnnotation]
                          .dropDuplicates("docId","annotSet","annotType","annotId","startOffset","endOffset")

    if (limit > 0) {
      results.limit(limit)
    } else {
      results
    }

  }

}
