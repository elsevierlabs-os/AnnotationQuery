package com.elsevier.aq.query

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Return the preceding sibling annotations for every annotation in the anchor Dataset[AQAnnotations]. 
 * The preceding sibling annotations can optionally be required to be contained in a container Dataset[AQAnnotations].  
 * The return type of this function is different from other functions.  
 * Instead of returning a Dataset[AQAnnotation] this function returns a Dataset[(AQAnnotation,Array[AQAnnotation])].
 */
object Preceding {
   
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  
  /*
   * annot - Dataset of AQAnnotations, the ones we will be using to look for preceding sibling annotations. 
   * anchor - Dataset of AQAnnotations  starting point for using to look for preceding sibling annotations (use the startOffset and docId).
   * container - Dataset of AQAnnotations to use when requiring the preceding sibling annotations to be contained in a specific annotation.
   * cnt - Number of preceding sibling AQAnnotations to return.
  */

  def apply(annot: Dataset[AQAnnotation], 
            anchor: Dataset[AQAnnotation], 
            container:Dataset[AQAnnotation]=spark.emptyDataset[AQAnnotation],
            cnt:Integer=3) : Dataset[(AQAnnotation,Array[AQAnnotation])] = {
  
    var results = annot.as("L").joinWith(anchor.as("R"),
                                         $"L.docId" === $"R.docId" &&
                                         $"L.endOffset" <= $"R.startOffset",
                                         "rightouter")
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
                                   if (entry._1 != null) lb += (entry._1)
                                 }
                                (key,lb.sortWith(_.endOffset > _.endOffset).toArray.slice(0,cnt))
                               })
  
    if (!container.rdd.isEmpty) {
      results.as("L").joinWith(container.as("R"),
                               ($"L._1.docId" === $"R.docId" &&
                                $"L._1.startOffset" >= $"R.startOffset" &&
                                $"L._1.endOffset" <= $"R.endOffset"),
                                "leftouter") 
                     .mapPartitions(recIt  => {
                         var containedResults: ListBuffer[(AQAnnotation,Array[AQAnnotation])] = ListBuffer[(AQAnnotation,Array[AQAnnotation])]()
                         while(recIt.hasNext) {
                           val rec = recIt.next()
                           // rec is of type ((AQAnnotation, Array[AQAnnotation]), AQAnnotation)
                           // Make sure we don't already have this entry
                           var found: Boolean = false
                           if (containedResults.size > 0) {
                             for (containedResult <- containedResults if found == false) {
                               if (rec._1._1.docId == containedResult._1.docId &&
                                   rec._1._1.annotSet == containedResult._1.annotSet &&
                                   rec._1._1.annotType == containedResult._1.annotType &&
                                   rec._1._1.annotId == containedResult._1.annotId &&
                                   rec._1._1.startOffset == containedResult._1.startOffset &&
                                   rec._1._1.endOffset == containedResult._1.endOffset &&
                                   rec._1._1.properties.getOrElse(Map.empty) == containedResult._1.properties.getOrElse(Map.empty)
                                   ) found = true
                             }
                           }
                           if (!found) {
                             var lb = new ListBuffer[AQAnnotation]
                             for (entry <- rec._1._2) {
                               if (rec._2 != null) {
                                 if (entry.startOffset >= rec._2.startOffset && entry.endOffset <= rec._2.endOffset) {
                                   lb += (entry)
                                 }
                               }
                             }
                             containedResults += ((rec._1._1,lb.toArray)) 
                           }
                           
                         }

                         containedResults.iterator
                     })

    } else {
      results
    }
    
  }
  
}