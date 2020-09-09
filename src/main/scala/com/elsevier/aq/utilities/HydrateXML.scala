package com.elsevier.aq.utilities

import java.net.URLDecoder
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.MutableList
import scala.io.BufferedSource

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
  
import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * This function will retrieve the xml for each AQAnnotation in the passed Dataset[AQAnnotation], populate the xml property with this value in the AQAnnotation, and return a Dataset[AQAnnotation] with the xml property populated.  
 */
class HydrateXML(spark: SparkSession)  {
  
  import spark.implicits._
  
  /*
   * ds - The Dataset of Annotations that we want to populate the xml property with the xml for this annotation
   * textPath - Path the str files.  The str files for the documents in the ds annotations must be found here.
   * om - The Dataset of OM Annotations (xml markup)
  */
  def apply(ds: Dataset[AQAnnotation], textPath: String, om: Dataset[AQAnnotation]): Dataset[AQAnnotation] = {
    
    val ATTR = "attr"
    val XML = "xml"
    
    // Join DS with OM
    var annotXML = ds.as("L").joinWith(om.as("R"),
                                      $"L.docId" === $"R.docId" &&
                                      $"L.startOffset" <= $"R.startOffset" &&
                                      $"L.endOffset" >= $"R.endOffset",
                                      "leftouter")                                
                               .groupByKey(rec => (rec._1.docId,rec._1.startOffset,rec._1.endOffset))
                               .mapGroups( (k,v) => {
                                 var key: AQAnnotation = null
                                 var lb = new ListBuffer[AQAnnotation]
                                 if (v.hasNext) {
                                   val entry = v.next
                                   key = entry._1
                                   if (entry._2 != null) lb += (entry._2)
                                 }
                                 while (v.hasNext) {
                                   val entry = v.next
                                   if (entry._2 != null) lb += (entry._2)
                                 }
                                (key,lb.toArray)
                               })   
       


    annotXML.sortWithinPartitions($"_1.docId",$"_1.startOffset",$"_1.endOffset").mapPartitions(recsIter => {
 
          val logger = org.apache.log4j.LogManager.getLogger("HydrateXML")
          var str= ""          
          var lastDoc= ""
    
          recsIter.map(rec => {
              var text = ""
              var currDoc = rec._1.docId
              if (!currDoc.equals(lastDoc)) { 
                  var source: Option[BufferedSource] = None               
                  str = try {
                           source = Some(scala.io.Source.fromFile(textPath + currDoc,"utf-8"))
                           source.get.mkString 
                        } catch {
                           case e: Exception => 
                             logger.error("Unable to find document: " + rec._1.docId)
                             ""               
                        } finally {
                           if (source != None) {
                             source.get.close()
                           } 
                        }
                  lastDoc = currDoc
              }
            
              // Did we find the document or is the 'xml' property already set?
              if (str == "" || (rec._1.properties.getOrElse(Map.empty).getOrElse(XML,"") != "")) {
                // Just return the original annotation 
                rec._1
              } else {
                val origText = str.substring(rec._1.startOffset.toInt, rec._1.endOffset.toInt)                
                // Get the OM tags for the annotation
                val omToks: MutableList[(Long,Long,Long,Long,String)] = MutableList[(Long,Long,Long,Long,String)]() 
                for (omTok <- rec._2) {
                  // Get the attributes for the omTok
                  var attrs = ""
                  if (omTok.properties.getOrElse(Map.empty).getOrElse(ATTR,"") != "") {
                    for (attrEntry <- omTok.properties.getOrElse(Map.empty).getOrElse(ATTR,"").split("&")) {
                      val attrNameValue = attrEntry.split("=")
                      attrs = attrs.concat(" " + attrNameValue(0) + "=\"" + URLDecoder.decode(attrNameValue(1),"utf-8") + "\"")
                    }
                  }
                  // Check if begin/end are the same and add only one entry
                  if (omTok.startOffset == omTok.endOffset) {
                    omToks += ((omTok.startOffset, omTok.startOffset,omTok.properties.getOrElse(Map.empty).getOrElse("parentId","0").toLong,omTok.annotId, "<" + omTok.annotType +  attrs + "/>"))
                  // Add begin/end tag for the entry
                  } else {
                    omToks += ((omTok.startOffset, omTok.startOffset, omTok.properties.getOrElse(Map.empty).getOrElse("parentId","0").toLong, omTok.annotId, "<" + omTok.annotType + attrs + ">"))
                    // use a negative value for the parentId on the end tag
                    omToks += ((omTok.endOffset, omTok.endOffset, -omTok.properties.getOrElse(Map.empty).getOrElse("parentId","0").toLong, omTok.annotId, "</" + omTok.annotType + ">"))
                  }
                }
                val sortedOMToks= omToks.sortBy(x => (x._1,x._2,x._3,x._4))  // Sort by startOffset, endOffset, parentId, annotId
                // Start stepping through the tags.  Add them to the buffer and substring from text.
                var modText = if (sortedOMToks.size == 0) {
                                origText
                              } else {
                                var txt = ""
                                var curOffset = rec._1.startOffset
                                sortedOMToks.foreach(entry => {
                                  // check if offset is less than current offset
                                  if (entry._1 <= curOffset) {
                                    txt = txt.concat(entry._5)
                                  } else {
                                    txt = txt.concat(origText.substring((curOffset - rec._1.startOffset).toInt,(entry._1 - rec._1.startOffset).toInt))
                                    txt = txt.concat(entry._5)
                                    curOffset = entry._2
                                  }
                                })
                                if (curOffset < rec._1.endOffset) {
                                  txt = txt.concat(origText.substring((curOffset - rec._1.startOffset).toInt))
                                }
                                txt
                              }                  

                var props = rec._1.properties.getOrElse(Map.empty)
                props += (XML -> modText)     
                new AQAnnotation(rec._1.docId,                       // Map to AQAnnotation
                                 rec._1.annotSet.toLowerCase,
                                 rec._1.annotType.toLowerCase,
                                 rec._1.startOffset,
                                 rec._1.endOffset,
                                 rec._1.annotId,
                                 Some(props))                
              }
          })
    })

  }
}