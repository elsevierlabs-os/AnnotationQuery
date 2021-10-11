package com.elsevier.aq.query

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.elsevier.aq.annotations.AQAnnotation
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row
  
/**
 * Provides the ability to create a string from a list of tokens that are contained in a span. The specified tokenProperty is used to extract the values from the tokens when creating the string. 
 * For SCNLP, this tokenProperty could be values like 'orig', 'lemma', or 'pos'. The spans would typically be a SCNLP 'sentence' or could even be things like an OM 'ce:para'.  
 * Returns a Dataset[AQAnnotation] spans with 3 new properties all prefixed with the specified tokenProperty value followed by (ToksStr, ToksSpos, ToksEpos) The ToksStr property will be the 
 * concatenated string of token property values contained in the span. The ToksSPos and ToksEpos are properties that will help us determine the start/end offset for each of the individual tokens in the ToksStr. 
 * These helper properties are needed for the function RegexTokensSpan so we can generate accurate accurate start/end offsets based on the str file.
 */

  class TokensSpan(spark: SparkSession) {
    
    import spark.implicits._
  
    /**
     * @param tokens Dataset of AQAnnotations (which we will use to concatenate for the string)
     * @param spans Dataset of AQAnnotations (identifies the start/end for the tokens to be used for the concatenated string)
     * @param tokenProperty The property field in the tokens to use for extracting the value for the concatenated string
     * @return Dataset[AQAnnotation]
    */
    def apply(tokens: Dataset[AQAnnotation], spans: Dataset[AQAnnotation], tokenProperty: String): Dataset[AQAnnotation] = {
      
      val ContainedInList = new ContainedInList(spark)
      
      ContainedInList(tokens,spans).toDF("span","tokens")
      .select($"span.*",$"tokens")
      .mapPartitions(recsIter => {
        var lb = ListBuffer[AQAnnotation]()
        recsIter.foreach(rec => {
          var newProps = Map[String,String]()
          var oldProps = rec.getAs[Map[String,String]]("properties")
          if (oldProps == null) oldProps = Map[String,String]()
          for ((k,v) <- oldProps) {
            newProps += (k -> v)
          }
          val toksStr = ListBuffer[String]()
          val toksSpos = ListBuffer[String]()
          val toksEpos = ListBuffer[String]()
          var offset = 0
          for (token <- rec.getAs[WrappedArray[Row]]("tokens")) {
            val tokStr = token.getAs[Map[String,String]]("properties").getOrElse(tokenProperty,"")
            toksStr += (tokStr)
            toksSpos += (offset.toString + "|" +token.getAs[Long]("startOffset").toString)
            offset += (tokStr.length)
            toksEpos += (offset.toString + "|" + token.getAs[Long]("endOffset").toString)
            offset += 1
          }
          newProps += (tokenProperty + "ToksStr" -> toksStr.mkString(" "))
          newProps += (tokenProperty + "ToksSpos" -> toksSpos.mkString(" "))
          newProps += (tokenProperty + "ToksEpos" -> toksEpos.mkString(" "))
          val annot = new AQAnnotation(rec.getAs[String]("docId"),                       // Map to AQAnnotation
                                       rec.getAs[String]("annotSet"),
                                       rec.getAs[String]("annotType"),
                                       rec.getAs[Long]("startOffset"),
                                       rec.getAs[Long]("endOffset"),
                                       rec.getAs[Long]("annotId"),
                                       Some(newProps))
            lb += (annot)
        })
        lb.iterator
      })
    }
  }