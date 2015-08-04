import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.ObjectInputStream
import java.util.Collection

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.services.kinesis.connectors.interfaces.ICollectionTransformer
import com.amazonaws.services.kinesis.model.Record

import BatchedKinesisMessageModelElasticsearchTransformer._
//remove if not needed
import scala.collection.JavaConversions._
import resource._ //use scala-arm from http://jsuereth.com/scala-arm/

/**
 * Extends KinesisMessageModelElasticsearchTransformer and implements ICollectionTransformer
 * to provide a toClass method to transform an Amazon Kinesis Record that contains multiple
 * instances of KinesisMessageModel.
 * 
 * To see how these records were batched, view {@class samples.BatchedStreamSource}.
 */

object BatchedKinesisMessageModelElasticsearchTransformer {

  private val LOG = LogFactory.getLog(classOf[BatchedKinesisMessageModelElasticsearchTransformer])
}

class BatchedKinesisMessageModelElasticsearchTransformer extends KinesisMessageModelElasticsearchTransformer with ICollectionTransformer[KinesisMessageModel, ElasticsearchObject] {

  override def toClass(record: Record): Collection[KinesisMessageModel] = {
    try {
      for (ois <- managed(new ObjectInputStream(new ByteArrayInputStream(record.getData.array())))){
        ois.readObject().asInstanceOf[Collection[KinesisMessageModel]]
      }
    } catch {
      case e: Exception => {
        val message = "Error reading object from ObjectInputStream: " + new String(record.getData.array())
        LOG.error(message, e)
        throw new IOException(message, e)
      }
    }
  }
}
