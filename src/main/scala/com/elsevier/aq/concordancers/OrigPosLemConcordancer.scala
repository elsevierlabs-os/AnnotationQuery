package com.elsevier.aq.concordancers

import java.net.URLDecoder
import scala.io.BufferedSource

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import com.elsevier.aq.annotations.AQAnnotation
  
/**
 * Output the string of text identified by the AQAnnotation (typically a sentence annotation). 
 * Below the sentence (in successive rows) output the original terms, parts of speech, and lemma terms for the text identified by the AQAnnotation.
 */
object OrigPosLemConcordancer {

  val logger = org.apache.log4j.LogManager.getLogger("OrigPosLemConcordancer")
  
  val spark = SparkSession.builder().getOrCreate()
  import spark.implicits._
  
  /*
   * sentences - Sentence annotations that you would like to display.  
   * annots - The Dataset of AQAnnotations that will contain the the AQAnnotations (orig, lemma, pos) for the above sentences
   * textPath - Path for the str files.  The sentence annotations must be for documents contained in these str files.
   * wordType -The annotType that identies the AQAnnotation in the above annotations.
   * nrows - Number of sentences to display
  */

  def apply(sentences: Dataset[AQAnnotation], annots:Dataset[AQAnnotation],  textPath: String, wordType:String="word", nrows:Integer=10): String = {

    val ORIG = "orig"
    val POS = "pos"
    val LEMMA = "lemma"
  
    val sentenceAnnots = sentences.sort("docId","startOffset").limit(nrows).collect
    var tmpStr = ""
    val docId = ""
    var str = ""
  
    // Get the TextAnnotations (for the specified annotType) for each sentence
    for(sentence <- sentenceAnnots) {
    val textAnnots = annots.filter($"docId" === sentence.docId &&
                                   $"annotType" === wordType &&
                                   $"startOffset" >= sentence.startOffset &&
                                   $"endOffset" <= sentence.endOffset)
                           .orderBy($"startOffset")
                           .collect()

    // Get the raw text for the sentence annotation
    var source: Option[BufferedSource] = None
    val text =  try {
                  if (docId != sentence.docId) {
                    source = Some(scala.io.Source.fromFile(textPath + sentence.docId, "utf-8"))
                    val str = source.get.mkString
                    str.substring(sentence.startOffset.toInt, sentence.endOffset.toInt)
                  }
                } catch {
                  case e: Exception =>  {
                    logger.error(e)
                    ""
                  }
                } finally {
                  if (source != None) {
                    source.get.close()
                  }        
                }    

    tmpStr += "<table border='1' style='font-family: monospace;table-layout: fixed;'><tr>"
    tmpStr += ("<td>" + sentence.docId + "</td>")
    tmpStr += ("<td>" + sentence.startOffset + "</td>")
    tmpStr += ("<td>" + sentence.endOffset + "</td>")
    tmpStr += ("<td colspan='" + textAnnots.size + "'>" + text + "</td>")
    tmpStr += "</tr>"
  
    // Get original row
    tmpStr += "<tr>"
    tmpStr += ("<td>orig</td>")
    tmpStr += ("<td bgcolor='grey'/>")
    tmpStr += ("<td bgcolor='grey'/>")
    for (annot <- textAnnots) {
      tmpStr += ("<td>" + URLDecoder.decode(annot.properties.getOrElse(Map.empty).getOrElse(ORIG,"UNKNOWN"),"utf-8") + "</td>")
    }
    tmpStr += "</tr>"

    // Get pos row
    tmpStr += "<tr>"
    tmpStr += ("<td>pos</td>")
    tmpStr += ("<td bgcolor='grey'/>")
    tmpStr += ("<td bgcolor='grey'/>")
    for (annot <- textAnnots) {
      tmpStr += ("<td>" + URLDecoder.decode(annot.properties.getOrElse(Map.empty).getOrElse(POS,"UNKNOWN"),"utf-8") + "</td>")
    }
    tmpStr += "</tr>"

    // Get lemma row
    tmpStr += "<tr>"
    tmpStr += ("<td>lemma</td>")
    tmpStr += ("<td bgcolor='grey'/>")
    tmpStr += ("<td bgcolor='grey'/>")
    for (annot <- textAnnots) {
      tmpStr += ("<td>" + URLDecoder.decode(annot.properties.getOrElse(Map.empty).getOrElse(LEMMA,"UNKNOWN"),"utf-8") + "</td>")
    }
    tmpStr += "</tr></table><p/><p/><p/>"
    }
    
   "<html><body>" + tmpStr + "</body></html>"
    
  }
 
}