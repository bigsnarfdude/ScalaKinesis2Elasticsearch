import KinesisConnectorExecutor
import KinesisMessageModel

import com.amazonaws.services.kinesis.connectors.KinesisConnectorRecordProcessorFactory
import com.amazonaws.services.kinesis.connectors.elasticsearch.ElasticsearchObject
import ElasticsearchExecutor._
//remove if not needed
import scala.collection.JavaConversions._

object ElasticsearchExecutor {

  private var configFile: String = "ElasticsearchSample.properties"

  def main(args: Array[String]) {
    Class.forName("org.elasticsearch.client.transport.TransportClient")
    Class.forName("org.apache.lucene.util.Version")
    val elasticsearchExecutor = new ElasticsearchExecutor[KinesisMessageModel, ElasticsearchObject](configFile)
    elasticsearchExecutor.run()
  }
}

class ElasticsearchExecutor(configFile: String) extends KinesisConnectorExecutor[KinesisMessageModel, ElasticsearchObject](configFile) {

  override def getKinesisConnectorRecordProcessorFactory(): KinesisConnectorRecordProcessorFactory[KinesisMessageModel, ElasticsearchObject] = {
    new KinesisConnectorRecordProcessorFactory[KinesisMessageModel, ElasticsearchObject](new ElasticsearchPipeline(), 
      config)
  }
}