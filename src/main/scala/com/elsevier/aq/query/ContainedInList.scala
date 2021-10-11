package com.elsevier.aq.query

  import org.apache.spark.sql.Dataset
  import org.apache.spark.sql.SparkSession
  import com.elsevier.aq.annotations.AQAnnotation
  import scala.collection.mutable.ListBuffer

 /**
  * Provide the ability to find annotations that are contained by another annotation.  The input is 2 Datasets of AQAnnotations.  We will call them A and B.  
  * The purpose is to find those annotations in A that are contained in B.  What that means is the start/end offset for an annotation from A  must be contained by the start/end offset from an annotation in  B.  
  * We of course have to also match on the document id.  
  * We ultimately return a Dataset with 2 fields where the first field is an annotation from B and the second field is an array of entries from A
  * that are contained in the first entry.   
  */
  class ContainedInList(spark: SparkSession) {
    
    import spark.implicits._
  
    /**
     * @param left Dataset of AQAnnotations, the ones we will return (as a list) if they are contained in AQAnnotations from 'right'.
     * @param right Dataset of AQAnnotations, the ones we are looking to see if they contain AQAnnotations from 'left'.
     * @return Dataset[AQAnnotation,Array[AQAnnotation]]
     */
    def apply(left: Dataset[AQAnnotation], right: Dataset[AQAnnotation]): Dataset[(AQAnnotation,Array[AQAnnotation])] = {
    
      left.as("L").joinWith(right.as("R"),
                            $"L.docId" === $"R.docId" &&
                            $"L.startOffset" >= $"R.startOffset" &&
                            $"L.endOffset" <= $"R.endOffset" &&
                            !($"L.annotSet" === $"R.annotSet" && $"L.annotType" === $"R.annotType"  && $"L.startOffset" === $"R.startOffset" && $"L.endOffset" === $"R.endOffset"))
                  .groupByKey(rec => (rec._2.docId,rec._2.startOffset,rec._2.endOffset))
                  .mapGroups( (k,v) => {
                    var key: AQAnnotation = null
                    var lb = new ListBuffer[AQAnnotation]
                    if (v.hasNext) {
                       val entry = v.next
                       key = entry._2
                       if (entry._1 != null) lb += (entry._1)
                    }
                    while (v.hasNext) {
                      val entry = v.next
                      lb += (entry._1)
                    }
                    (key,lb.sortWith(_.startOffset < _.startOffset).toArray)
                   })    
    }
  }