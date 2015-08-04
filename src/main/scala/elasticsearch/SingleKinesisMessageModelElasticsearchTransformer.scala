import java.io.IOException
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer
import com.amazonaws.services.kinesis.model.Record
import com.fasterxml.jackson.databind.ObjectMapper
import SingleKinesisMessageModelElasticsearchTransformer._
//remove if not needed
import scala.collection.JavaConversions._

/**
 * Extends KinesisMessageModelElasticsearchTransformer and implements ITransformer
 * to provide a toClass method to transform an Amazon Kinesis Record into a KinesisMessageModel.
 * 
 * To see how this record was put, view {@class samples.StreamSource}.
 */


object SingleKinesisMessageModelElasticsearchTransformer {

  private val LOG = LogFactory.getLog(classOf[SingleKinesisMessageModelElasticsearchTransformer])
}

class SingleKinesisMessageModelElasticsearchTransformer extends KinesisMessageModelElasticsearchTransformer with ITransformer[KinesisMessageModel, ElasticsearchObject] {

  override def toClass(record: Record): KinesisMessageModel = {
    try {
      new ObjectMapper().readValue(record.getData.array(), classOf[KinesisMessageModel])
    } catch {
      case e: IOException => {
        val message = "Error parsing record from JSON: " + new String(record.getData.array())
        LOG.error(message, e)
        throw new IOException(message, e)
      }
    }
  }
}