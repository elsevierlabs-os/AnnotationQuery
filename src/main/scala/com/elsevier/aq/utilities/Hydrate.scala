package com.elsevier.aq.utilities

import java.net.URLDecoder
import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
  
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * This function will retrieve the text for each AQAnnotation in the passed Dataset[AQAnnotation], populate the text property with this value in the AQAnnotation, and return a Dataset[AQAnnotation] with the text property populated.  
 * Keep in mind that for 'text/word' annotations the orig column will already be populated with the 'original' text value so this may not be needed.  
 * However, if you are working with sentence annotations (and other similar annotations) this could prove to be very helpful. 
 */
class Hydrate(spark: SparkSession)  {
  
  import spark.implicits._
  
  /*
   * ds - The Dataset of Annotations that we want to populate the text property with the text for this annotation
   * textPath - Path the str files.  The str files for the documents in the ds annotations must be found here.
   * excludes - Whether we want to include the 'excludes' text.  True means exclude the excluded text.
  */
  def apply(ds: Dataset[AQAnnotation], textPath: String, excludes: Boolean=true): Dataset[AQAnnotation] = {
    
    val TEXT = "text"
    val EXCLUDES = "excludes"   
    
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
    
    results = ds.sortWithinPartitions($"docId").mapPartitions(recsIter => {
 
          val logger = org.apache.log4j.LogManager.getLogger("Hydrate")
          var str= ""          
          var lastDoc= ""
    
          recsIter.map(li => {
              var text = ""
              var currDoc = li.docId
              if (!currDoc.equals(lastDoc)) { 
                  var source: Option[BufferedSource] = None               
                  str = try {
                           source = Some(scala.io.Source.fromFile(textPath + currDoc,"utf-8"))
                           source.get.mkString 
                        } catch {
                           case e: Exception => 
                             logger.error("Unable to find document: " + li.docId)
                             ""               
                        } finally {
                           if (source != None) {
                             source.get.close()
                           } 
                        }
                  lastDoc = currDoc
              }
            
              // Did we find the document or is the 'text' property already set?
              if (str == "" || (li.properties.getOrElse(Map.empty).getOrElse(TEXT,"") != "")) {
                // Just return the original annotation 
                li
              } else {
                
                // Check the excludes flag
                if (excludes && li.properties.getOrElse(Map.empty).getOrElse(EXCLUDES,"") != "") {
                  // Check for excludes (startOffset,endOffset)
                  var exList:ListBuffer[(Integer,Integer)] = ListBuffer[(Integer,Integer)]()
                  // (annotId,annotSet,annotType,startOffset,endOffset)
                  var lb = new ListBuffer[(Long,String,String,Long,Long)]
                  for (excludesEntry <- li.properties.getOrElse(Map.empty).getOrElse(EXCLUDES,"").split("\\|")) {
                    var excToks = excludesEntry.split(",")
                    lb += ((excToks(0).toLong,excToks(1),excToks(2),excToks(3).toLong,excToks(4).toLong))
                  }
                  val excludes = lb.distinct.toArray
                  excludes.foreach(entry => {
                    // (startOffset,endOffset)
                    exList += ((entry._4.toInt,entry._5.toInt))
                  })
                  exList.sortBy(x => (x._1,x._2))
                  // Process the excludes 
                  var curOffset: Integer = li.startOffset.toInt
                  for (exclude <- exList) {
                    if (exclude._1 <= curOffset) {
                      curOffset = exclude._2
                    } else {
                      text = text + str.substring(curOffset,exclude._1)
                      curOffset = exclude._2
                    }
                  }
                  if (curOffset < li.endOffset) {
                    text = text + str.substring(curOffset, li.endOffset.toInt)
                  }
                } else {
                  // Excludes flag was not set or there were no excludes present
                  text = str.substring(li.startOffset.toInt, li.endOffset.toInt)
                }
                var props = li.properties.getOrElse(Map.empty)
                props += (TEXT -> text)
                new AQAnnotation(li.docId,                       // Map to AQAnnotation
                                 li.annotSet.toLowerCase,
                                 li.annotType.toLowerCase,
                                 li.startOffset,
                                 li.endOffset,
                                 li.annotId,
                                 Some(props))             
          }
        })
    })
    results
  }
  
}