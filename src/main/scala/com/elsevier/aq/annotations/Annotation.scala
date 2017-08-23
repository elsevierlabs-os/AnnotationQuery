package com.elsevier.aq.annotations

/** CATAnnotation **/
case class CATAnnotation(docId: String,                 // Document Id (PII)
                         annotSet: String,              // Annotation set (such as scnlp, ge)
                         annotType: String,             // Annotation type (such as text, sentence)
                         startOffset: Long,             // Starting offset for the annotation (based on the text file for the document)
                         endOffset: Long,               // Ending offset for the annotation (based on the text file for the document)
                         annotId: Long,                 // Annotation Id (after the annotations have been reordered)
                         other: Option[String] = None)  // Contains any attributes such as exclude annotations, original annotation id, parent id, etc.

  
/** AQAnnotation **/
case class AQAnnotation(docId: String,                                   // Document Id (PII)
                         annotSet: String,                               // Annotation set (such as scnlp, ge)
                         annotType: String,                              // Annotation type (such as text, sentence)
                         startOffset: Long,                              // Starting offset for the annotation (based on the text file for the document)
                         endOffset: Long,                                // Ending offset for the annotation (based on the text file for the document)
                         annotId: Long,                                  // Annotation Id
                         properties: Option[scala.collection.Map[String,String]] = None)  // Properties