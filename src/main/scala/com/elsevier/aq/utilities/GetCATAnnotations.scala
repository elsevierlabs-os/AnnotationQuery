package com.elsevier.aq.utilities

import java.net.URLEncoder
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession

import com.elsevier.aq.annotations.AQAnnotation
import com.elsevier.aq.annotations.CATAnnotation
   
/**
 * This function converts a Dataset[AQAnnotation] to a Dataset[CATAnnotation].  
 * If specific properties (name-value pairs to set in the CATAnnotation other column) are desired, you have the option of specifying an Array of names (for these name-value pairs). 
 * Additionally, you have the option of specifying if the values for these name-value pairs that  should be url encoded.
 */
class GetCATAnnotations(spark: SparkSession) {
  
  import spark.implicits._
  
  /*
   * aqAnnotations - Dataset of AQAnnotations to convert to Dataset of CATAnnotations
   * props - Array of property names  to make name-value pairs in the other column of CATAnnotation.
   * encodeProps - Array of property names  to url encode the value when making name-value pairs in the other column of CATAnnotation.
  */

  def apply(aqAnnotations:Dataset[AQAnnotation],  props:Array[String]=Array.empty[String], encodeProps:Array[String]=Array.empty[String]): Dataset[CATAnnotation] = {

    val WILDCARD = "*"
    
    aqAnnotations.map(aqAnnotation => {
    
      var otherBuf:ListBuffer[String] = new ListBuffer[String]()
    
      for((key:String,value:String) <- aqAnnotation.properties.getOrElse(Map[String,String]())) {
      
        if (props.contains(key) || props.contains(WILDCARD)) {
          if (encodeProps.contains(key) || encodeProps.contains(WILDCARD)) {
            otherBuf += (key + "=" + URLEncoder.encode(value,"UTF-8"))
          } else {
            otherBuf += (key + "=" + value)
          }
        } 
      }
    
      CATAnnotation(aqAnnotation.docId,
                 aqAnnotation.annotSet,
                 aqAnnotation.annotType,
                 aqAnnotation.startOffset,
                 aqAnnotation.endOffset,
                 aqAnnotation.annotId,
                 if (otherBuf.size > 0) Some(otherBuf.mkString("&")) else None)
    }) 
  }
  
}