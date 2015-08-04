import com.amazonaws.services.kinesis.metrics.impl.CWMetricsFactory
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory

//remove if not needed
import scala.collection.JavaConversions._

abstract class KinesisConnectorMetricsExecutor[T, U](configFile: String) extends KinesisConnectorExecutor[T, U](configFile) {

  val mFactory = new CWMetricsFactory(config.AWS_CREDENTIALS_PROVIDER, config.CLOUDWATCH_NAMESPACE, config.CLOUDWATCH_BUFFER_TIME, 
    config.CLOUDWATCH_MAX_QUEUE_SIZE)

  super.initialize(config, mFactory)
}