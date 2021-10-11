package com.elsevier.aq.utilities

import scala.collection.mutable.ListBuffer
import scala.io.BufferedSource

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
  
import com.elsevier.aq.annotations.AQAnnotation
  /**
 * This function will calculate the xpath expression for each Original Markup (OM) AQAnnotation in the passed OM Dataset[AQAnnotation], populate the xpath property with this value in the AQAnnotation, and return a Dataset[AQAnnotation] with the xpath property populated.  
 */
class XPath(spark: SparkSession)  {
  
  import spark.implicits._
  
  /**
   * @param ds The Dataset of OM Annotations that we want to populate the xpath property for this om annotation
   * @param omBase Path for the individual OM annotation files.  The OM files for the documents in the ds annotations must be found here.
   * @return Dataset[AQAnnotation]
  */
  def apply(ds: Dataset[AQAnnotation], omBase: String): Dataset[AQAnnotation] = {
    
    //  Get the Caret annotations for the specified OM file
    def getCaretFileAnnots(omBase: String, filename: String) :Array[String] = {
      import java.nio.file.{Files, Paths}
      import java.util.zip.GZIPInputStream
      import java.io._
      import org.apache.commons.io.IOUtils
    
      var byteArray: Option[Array[Byte]] = None
      var result = ""
      
      try {
        byteArray = Some(Files.readAllBytes(Paths.get(omBase + filename)))
      } catch {
        case e: Exception =>  System.out.println("Problems reading file.")
      }

      if (byteArray != None) {
        val magic: Int = byteArray.get(0) & 0xff | (byteArray.get(1) << 8) & 0xff00
        if (magic == GZIPInputStream.GZIP_MAGIC) {
          // File is gzipped
          try {
            val gis = new GZIPInputStream(new ByteArrayInputStream(byteArray.get))
            val bufferedReader = new BufferedReader(new InputStreamReader(gis, "UTF-8"))
            result =IOUtils.toString(bufferedReader)
          } catch {
            case e: Exception =>  System.out.println("Problems decompressing file." + e.getMessage) 
          }
        } else {
          // File is not gzipped
          val bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(byteArray.get),"UTF-8"))
          result = IOUtils.toString(bufferedReader)
        }
      }
      result.split("\n")
    } 
    
    // Get the om annotations with the same annotType (element name) and  parentId.  This indicates they are siblings as they have the same parent.
    def getSibling(target:com.elsevier.aql.annotations.AQAnnotation, omAnnots: Array[com.elsevier.aql.annotations.AQAnnotation]): String = {
      var xpath = target.annotType
      var idx = 1
      val name = target.annotType
      val parentId = target.properties.getOrElse(Map.empty).getOrElse("parentId","")
      val siblings = com.elsevier.aql.query.FilterProperty(com.elsevier.aql.query.FilterType(omAnnots,name),"parentId",parentId)
      var found = false
      for (sibling <- siblings if !found) {
        if (sibling.startOffset == target.startOffset && sibling.endOffset == target.endOffset) {
          found = true
        } else {
          idx += 1
        }
      }
      return xpath + "[" + idx + "]"
    }  
    
    // Calculate the xpath expression for the annotation
    def getXPath(target: com.elsevier.aql.annotations.AQAnnotation, omAnnots: Array[com.elsevier.aql.annotations.AQAnnotation]): String = {
      if (omAnnots.size == 0) return "ERR"
      val xPathBuff : ListBuffer[String] = new ListBuffer[String]()
      val targetOrigAnnotID = target.properties.getOrElse(Map.empty).getOrElse("origAnnotID","").toInt
      val parents = com.elsevier.aql.query.BinaryOp(omAnnots,
                                                    Array(target),    
                                                    (l, r) => l.startOffset <= r.startOffset &&
                                                            l.endOffset >= r.endOffset &&
                                                            l.properties.getOrElse(Map.empty).getOrElse("parentId","0").toInt < r.properties.getOrElse(Map.empty).getOrElse("parentId","0").toInt)
      //val parents = com.elsevier.aql.query.Contains(omAnnots,Array(target)).  Can't use the default Contains so reimpmlemented above a function that meets our needs.
      for (parent <- parents) {
        // Make sure parent is <= target origAnnotId
        if (parent.properties.getOrElse(Map.empty).getOrElse("parentId","0").toInt < targetOrigAnnotID) {
          xPathBuff += (getSibling(parent,omAnnots))
        }
      }
      return "/" + xPathBuff.mkString("/") + "/" + getSibling(target,omAnnots)
    }
    
    var results:Dataset[AQAnnotation] = spark.emptyDataset[AQAnnotation]
    
    results = ds.sortWithinPartitions($"docId").mapPartitions(recsIter => { 
      var lastDoc= ""
      var omAnnots: Array[com.elsevier.aql.annotations.AQAnnotation] = Array()
      recsIter.map(rec => {  
        var currDoc = rec.docId
        if (!currDoc.equals(lastDoc)) {          
          omAnnots = com.elsevier.aql.utilities.GetAQAnnotations(getCaretFileAnnots(omBase,rec.docId),rec.docId,Array("*"),Array("*"),Array("*"))
          lastDoc = currDoc
        }
        
        var xpath = ""
        try {
          val annotProps = rec.properties.getOrElse(Map.empty).toMap
          val annotation = com.elsevier.aql.annotations.AQAnnotation(rec.docId,
                                                                     rec.annotSet,
                                                                     rec.annotType,
                                                                     rec.startOffset,
                                                                     rec.endOffset,
                                                                     rec.annotId,
                                                                     Some(annotProps))
          xpath = getXPath(annotation,omAnnots)
        } catch {
          case e: Exception => System.out.println(e.printStackTrace)
        }
        
               
        var props = rec.properties.getOrElse(Map.empty)
        props += ("xpath" -> xpath)
        new AQAnnotation(rec.docId,                       // Map to AQAnnotation
                         rec.annotSet,
                         rec.annotType,
                         rec.startOffset,
                         rec.endOffset,
                         rec.annotId,
                         Some(props))
      })
    })
    results
  }
}