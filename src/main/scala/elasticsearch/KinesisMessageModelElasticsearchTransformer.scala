import java.io.IOException

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory;

import KinesisMessageModel

import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchTransformer
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Extends ElasticsearchTransformer for {@link KinesisMessageModel}. Provides implementation for fromClass by
 * transforming the record into JSON format and setting the index, type and id to use for Elasticsearch.
 * 
 * Abstract to give same implementation of fromClass for both Batched and Single processing scenarios.
 */
import KinesisMessageModelElasticsearchTransformer._
//remove if not needed
import scala.collection.JavaConversions._

object KinesisMessageModelElasticsearchTransformer {

  private val LOG = LogFactory.getLog(classOf[KinesisMessageModelElasticsearchTransformer])

  private val INDEX_NAME = "kinesis-example44"
}

abstract class KinesisMessageModelElasticsearchTransformer extends ElasticsearchTransformer[KinesisMessageModel] {

  override def fromClass(record: KinesisMessageModel): ElasticsearchObject = {
    val index = INDEX_NAME
    val `type` = record.getClass.getSimpleName
    val id = java.lang.Integer.toString(record.getUserid)
    var source: String = null
    val create = true
    try {
      source = new ObjectMapper().writeValueAsString(record)
    } catch {
      case e: JsonProcessingException => {
        val message = "Error parsing record to JSON"
        LOG.error(message, e)
        throw new IOException(message, e)
      }
    }
    val elasticsearchObject = new ElasticsearchObject(index, `type`, id, source)
    elasticsearchObject.setCreate(create)
    elasticsearchObject
  }
}
