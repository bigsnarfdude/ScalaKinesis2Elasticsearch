import KinesisMessageModel

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchEmitter
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformerBase
//remove if not needed
import scala.collection.JavaConversions._

class ElasticsearchPipeline extends IKinesisConnectorPipeline[KinesisMessageModel, ElasticsearchObject] {

  override def getEmitter(configuration: KinesisConnectorConfiguration): IEmitter[ElasticsearchObject] = {
    new ElasticsearchEmitter(configuration)
  }

  override def getBuffer(configuration: KinesisConnectorConfiguration): IBuffer[KinesisMessageModel] = {
    new BasicMemoryBuffer[KinesisMessageModel](configuration)
  }

  override def getTransformer(configuration: KinesisConnectorConfiguration): ITransformerBase[KinesisMessageModel, ElasticsearchObject] = {
    if (configuration.BATCH_RECORDS_IN_PUT_REQUEST) {
      new BatchedKinesisMessageModelElasticsearchTransformer()
    } else {
      new SingleKinesisMessageModelElasticsearchTransformer()
    }
  }

  override def getFilter(configuration: KinesisConnectorConfiguration): IFilter[KinesisMessageModel] = {
    new AllPassFilter[KinesisMessageModel]()
  }
}