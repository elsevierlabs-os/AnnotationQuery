package com.elsevier.aq.concordancers

import scala.collection.mutable.MutableList
import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import com.elsevier.aq.annotations.AQAnnotation 
  
/**
 * Output the string of text identified by the AQAnnotation and highlight in 'red' the text that was ignored (excluded).
 */
class Concordancer(spark: SparkSession) {

  val logger = org.apache.log4j.LogManager.getLogger("Concordancer")
  
  import spark.implicits._
  
  /**
   * @param results Dataset[AQAnnotation] that you would like to display.  
   * @param textPath Path for the str files.  The annotations in results must be for documents contained in these str files.
   * @param nrows Number of results to display
   * @param offset Number of characters before/after each annotation in results to display
   * @param highlightAnnotations Dataset[AQAnnotation] that you would like to highlight in the results
   * @param colorPropertyKey Key in the property map of highlightAnnotations to get the value for the color lookup in the specified colorMap
   * @param colorMap Map the colorPropertyKey value to the specified color in the Map.  Default is blue when not found.
   * @return String of html
   */
    
  def apply(results: Dataset[AQAnnotation], textPath: String, nrows:Integer=10, offset:Integer=0, highlightAnnotations:Dataset[AQAnnotation] =  spark.emptyDataset[AQAnnotation],
            colorPropertyKey: String = "", colorMap: Map[String,String] = Map.empty[String, String]): String = {

    val EXCLUDES = "excludes"
    
    val limResults = results.sort("docId","startOffset").limit(nrows)
  
    // Get the Annotations to be highlighted
    val highlightTokens = limResults.as("L").join(highlightAnnotations.as("R"),
                                               ($"L.docId" === $"R.docId" &&
                                                $"L.startOffset" <= $"R.startOffset" &&
                                                $"L.endOffset" >= $"R.endOffset"))
                                         .select($"R.*")
                                         .as[AQAnnotation]
                                         .sort("startOffset","annotType")
                                         .collect
  
    val strsLis = limResults.take(nrows).map(li => {
      var source: Option[BufferedSource] = None
      try {
        source = Some(scala.io.Source.fromFile(textPath + li.docId, "utf-8"))
        val str = source.get.mkString
        // Get the text for annotation (account for the offset)
        val origText = str.substring(li.startOffset.toInt, li.endOffset.toInt)
      
        val hlToks = highlightTokens.filter(rec => rec.docId == li.docId && rec.startOffset >= li.startOffset && rec.endOffset <= li.endOffset)
                        .flatMap(rec => {
                          //(startOffset,endOffset,type,text)
                          val entries:MutableList[(Long,Long,String,String)] = MutableList[(Long,Long,String,String)]()

                          // Add begin/end tag for the entry
                          entries += ((rec.startOffset, rec.startOffset, "hl", "<font color='" + colorMap.getOrElse(rec.properties.getOrElse(Map.empty).getOrElse(colorPropertyKey,""),"blue") + "'>"))
                          entries += ((rec.endOffset, rec.endOffset, "hl", "</font>"))

                          entries.toIterator
                        })
      
        //Get any exclude annotations
        var exList:MutableList[(Long,Long,String,String)] = MutableList[(Long,Long,String,String)]()
        if (li.properties.getOrElse(Map.empty).getOrElse(EXCLUDES,"") != "") {
           // (annotId,annotSet,annotType,startOffset,endOffset)
           var lb = new ListBuffer[(Long,String,String,Long,Long)]
           for (excludesEntry <- li.properties.getOrElse(Map.empty).getOrElse(EXCLUDES,"").split("\\|")) {
              var excToks = excludesEntry.split(",")
              lb += ((excToks(0).toLong,excToks(1),excToks(2),excToks(3).toLong,excToks(4).toLong))
           }
           val excludes = lb.distinct.toArray
           excludes.foreach(entry => {
             exList += ((entry._4,entry._5,"xcl",""))
           })
        } 
        val exToks = exList.toArray
      
        // Combine Original Markup and Exclude annotations. Sort by startOffset, endOffset, type
        val allToks = (hlToks ++ exToks).sortBy(x => (x._1,x._2,x._3))
      
        // Start stepping through the tags.  Add them to the buffer and substring from text.
        var modText = if (allToks.size == 0) {
                        origText
                      } else {
                        var txt = ""
                        var curOffset = li.startOffset
                        allToks.foreach(entry => {
                          // check if offset is less than current offset
                          if (entry._1 <= curOffset) {
                            if (entry._3 == "hl") {
                              txt = txt.concat(entry._4)
                            } else {
                              txt = txt +
                                  " <font color='red'>" + 
                                  origText.substring((entry._1 - li.startOffset).toInt,(entry._2 - li.startOffset).toInt) +
                                  "</font> " 
                              curOffset = entry._2
                            }
                          } else {
                            txt = txt.concat(origText.substring((curOffset - li.startOffset).toInt,(entry._1 - li.startOffset).toInt))
                            if (entry._3 == "hl") {
                              txt = txt.concat(entry._4)
                            } else {
                               txt = txt +
                                  "<font color='red'>" + 
                                  origText.substring((entry._1 - li.startOffset).toInt,(entry._2 - li.startOffset).toInt) +
                                  "</font>"                             
                            }
                            curOffset = entry._2
                          }
                        })
                        if (curOffset < li.endOffset) {
                          txt = txt.concat(origText.substring((curOffset - li.startOffset).toInt))
                        }
                        txt
                      }
        
        val sab = if (offset != 0) "<font color='green'> &gt; </font>" else ""
        val eab = if (offset != 0) "<font color='green'> &lt; </font>" else ""
        
        "<tr><td>" + li.docId+"</td>" + 
        "<td>" + li.annotSet+"</td>" +
        "<td>" + li.annotType+"</td><td><div style='word-break:break-all;'>" +
        str.substring(Math.max(0,(li.startOffset - offset).toInt), li.startOffset.toInt) + 
        sab + 
        modText + 
        eab  + 
        str.substring(li.endOffset.toInt, Math.min(str.length,(li.endOffset + offset).toInt)) + 
        "</div></td></tr>"
       } catch {
         case e: Exception => logger.error(e)
         "<tr><td>" + li.docId+"</td>" + 
         "<td>" + li.annotSet+"</td>" +
         "<td>" + li.annotType+"</td><td><div style='word-break:break-all;'>" +
         "</div></td></tr>"
       } finally {
         if (source != None) {
          source.get.close()
         }
       }        
    })
    val tmpStr = strsLis.mkString("\n")
    "<html><body><table border='1' style='font-family: monospace;table-layout: fixed;'>" + tmpStr + "</table></body></html>"
  
  }

}