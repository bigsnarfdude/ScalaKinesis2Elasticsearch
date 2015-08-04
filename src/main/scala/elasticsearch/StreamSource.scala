import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.ByteBuffer

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import KinesisUtils

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.fasterxml.jackson.databind.ObjectMapper

import StreamSource._
//remove if not needed
import scala.collection.JavaConversions._
import resource._ //use scala-arm from http://jsuereth.com/scala-arm/


/**
 * This class is a data source for supplying input to the Amazon Kinesis stream. It reads lines from the
 * input file specified in the constructor and emits them by calling String.getBytes() into the
 * stream defined in the KinesisConnectorConfiguration.
 */

object StreamSource {

  private var LOG: Log = LogFactory.getLog(classOf[StreamSource])
}

class StreamSource(protected var config: KinesisConnectorConfiguration, protected val inputFile: String, loopOverStreamSource: Boolean)
    extends Runnable {

  protected var kinesisClient: AmazonKinesisClient = new AmazonKinesisClient(config.AWS_CREDENTIALS_PROVIDER)
  protected val loopOverInputFile = loopOverStreamSource
  protected var objectMapper: ObjectMapper = new ObjectMapper()
  kinesisClient.setRegion(RegionUtils.getRegion(config.REGION_NAME))

  if (config.KINESIS_ENDPOINT != null) {
    kinesisClient.setEndpoint(config.KINESIS_ENDPOINT)
  }

  KinesisUtils.createInputStream(config)

  def this(config: KinesisConnectorConfiguration, inputFile: String) {
    this(config, inputFile, false)
  }

  override def run() {
    var iteration = 0
    do {
      val inputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(inputFile)
      if (inputStream == null) {
        throw new IllegalStateException("Could not find input file: " + inputFile)
      }
      if (loopOverInputFile) {
        LOG.info("Starting iteration " + iteration + " over input file.")
      }
      try {
        processInputStream(inputStream, iteration)
      } catch {
        case e: IOException => {
          LOG.error("Encountered exception while putting data in source stream.", e)
          //break
        }
      }
      iteration += 1
    } while (loopOverInputFile);
  }

  protected def processInputStream(inputStream: InputStream, iteration: Int) {
    for (br <- managed(new BufferedReader(new InputStreamReader(inputStream)))){
      var line: String = null
      var lines = 0
      while ((line = br.readLine()) != null) {
        val kinesisMessageModel = objectMapper.readValue(line, classOf[KinesisMessageModel])
        val putRecordRequest = new PutRecordRequest()
        putRecordRequest.setStreamName(config.KINESIS_INPUT_STREAM)
        putRecordRequest.setData(ByteBuffer.wrap(line.getBytes))
        putRecordRequest.setPartitionKey(java.lang.Integer.toString(kinesisMessageModel.getUserid))
        kinesisClient.putRecord(putRecordRequest)
        lines += 1
      }
      LOG.info("Added " + lines + " records to stream source.")
    }
  }

  protected override def finalize() {
    super.finalize()
  }
}
