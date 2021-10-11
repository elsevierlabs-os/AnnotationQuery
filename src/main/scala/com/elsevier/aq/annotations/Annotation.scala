package com.elsevier.aq.annotations

/** CATAnnotation.
 *  @param docId Document Id (PII)
 *  @param annotSet Annotation set (such as scnlp, ge)
 *  @param annotType Annotation type (such as text, sentence)
 *  @param startOffset Starting offset for the annotation (based on the text file for the document)
 *  @param endOffset Ending offset for the annotation (based on the text file for the document)
 *  @param annotId Annotation Id (after the annotations have been reordered)
 *  @param other Contains any attributes such as exclude annotations, original annotation id, parent id, etc.
*/
case class CATAnnotation(docId: String,                
                         annotSet: String,             
                         annotType: String,            
                         startOffset: Long,           
                         endOffset: Long,              
                         annotId: Long,               
                         other: Option[String] = None) 

  
/** AQAnnotation.
 *  @param docId Document Id (PII)
 *  @param annotSet Annotation set (such as scnlp, ge)
 *  @param annotType Annotation type (such as text, sentence)
 *  @param startOffset Starting offset for the annotation (based on the text file for the document)
 *  @param endOffset Ending offset for the annotation (based on the text file for the document)
 *  @param annotId Annotation Id (after the annotations have been reordered)
 *  @param properties Map of key-value properties
*/
case class AQAnnotation(docId: String,                                  
                         annotSet: String,                              
                         annotType: String,                             
                         startOffset: Long,                             
                         endOffset: Long,                               
                         annotId: Long,                                  
                         properties: Option[scala.collection.Map[String,String]] = None)  // Properties