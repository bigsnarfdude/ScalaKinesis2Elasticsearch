import java.util.List

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.model.CreateStreamRequest
import com.amazonaws.services.kinesis.model.DeleteStreamRequest
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.services.kinesis.model.ListStreamsRequest
import com.amazonaws.services.kinesis.model.ListStreamsResult
import com.amazonaws.services.kinesis.model.ResourceNotFoundException

/**
 * Utilities to create and delete Amazon Kinesis streams.
 */
//remove if not needed
import scala.collection.JavaConversions._

object KinesisUtils {

  private var LOG: Log = LogFactory.getLog(classOf[KinesisUtils])

  def createInputStream(config: KinesisConnectorConfiguration) {
    val kinesisClient = new AmazonKinesisClient(config.AWS_CREDENTIALS_PROVIDER)
    kinesisClient.setRegion(RegionUtils.getRegion(config.REGION_NAME))
    if (config.KINESIS_ENDPOINT != null) {
      kinesisClient.setEndpoint(config.KINESIS_ENDPOINT)
    }
    createAndWaitForStreamToBecomeAvailable(kinesisClient, config.KINESIS_INPUT_STREAM, config.KINESIS_INPUT_STREAM_SHARD_COUNT)
  }

  def createOutputStream(config: KinesisConnectorConfiguration) {
    val kinesisClient = new AmazonKinesisClient(config.AWS_CREDENTIALS_PROVIDER)
    kinesisClient.setRegion(RegionUtils.getRegion(config.REGION_NAME))
    if (config.KINESIS_ENDPOINT != null) {
      kinesisClient.setEndpoint(config.KINESIS_ENDPOINT)
    }
    createAndWaitForStreamToBecomeAvailable(kinesisClient, config.KINESIS_OUTPUT_STREAM, config.KINESIS_OUTPUT_STREAM_SHARD_COUNT)
  }

  def createAndWaitForStreamToBecomeAvailable(kinesisClient: AmazonKinesisClient, streamName: String, shardCount: Int) {
    if (streamExists(kinesisClient, streamName)) {
      val state = streamState(kinesisClient, streamName)
      state match {
        case "DELETING" => 
          var startTime = System.currentTimeMillis()
          var endTime = startTime + 1000 * 120
          while (System.currentTimeMillis() < endTime && streamExists(kinesisClient, streamName)) {
            try {
              LOG.info("...Deleting Stream " + streamName + "...")
              Thread.sleep(1000 * 10)
            } catch {
              case e: InterruptedException => 
            }
          }
          if (streamExists(kinesisClient, streamName)) {
            LOG.error("KinesisUtils timed out waiting for stream " + streamName + 
              " to delete")
            throw new IllegalStateException("KinesisUtils timed out waiting for stream " + streamName + 
              " to delete")
          }

        case "ACTIVE" => 
          LOG.info("Stream " + streamName + " is ACTIVE")
          return

        case "CREATING" => //break
        case "UPDATING" => 
          LOG.info("Stream " + streamName + " is UPDATING")
          return

        case _ => throw new IllegalStateException("Illegal stream state: " + state)
      }
    } else {
      val createStreamRequest = new CreateStreamRequest()
      createStreamRequest.setStreamName(streamName)
      createStreamRequest.setShardCount(shardCount)
      kinesisClient.createStream(createStreamRequest)
      LOG.info("Stream " + streamName + " created")
    }
    val startTime = System.currentTimeMillis()
    val endTime = startTime + (10 * 60 * 1000)
    while (System.currentTimeMillis() < endTime) {
      try {
        Thread.sleep(1000 * 10)
      } catch {
        case e: Exception => 
      }
      val streamStatus = streamState(kinesisClient, streamName)
      if (streamStatus == "ACTIVE") {
        LOG.info("Stream " + streamName + " is ACTIVE")
        return
      }
    }
  }

  private def streamExists(kinesisClient: AmazonKinesisClient, streamName: String): Boolean = {
    val describeStreamRequest = new DescribeStreamRequest()
    describeStreamRequest.setStreamName(streamName)
    try {
      kinesisClient.describeStream(describeStreamRequest)
      true
    } catch {
      case e: ResourceNotFoundException => false
    }
  }

  private def streamState(kinesisClient: AmazonKinesisClient, streamName: String): String = {
    val describeStreamRequest = new DescribeStreamRequest()
    describeStreamRequest.setStreamName(streamName)
    try {
      kinesisClient.describeStream(describeStreamRequest)
        .getStreamDescription
        .getStreamStatus
    } catch {
      case e: AmazonServiceException => null
    }
  }

  def listAllStreams(kinesisClient: AmazonKinesisClient): List[String] = {
    val listStreamsRequest = new ListStreamsRequest()
    listStreamsRequest.setLimit(10)
    var listStreamsResult = kinesisClient.listStreams(listStreamsRequest)
    val streamNames = listStreamsResult.getStreamNames
    while (listStreamsResult.isHasMoreStreams) {
      if (streamNames.size > 0) {
        listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size - 1))
      }
      listStreamsResult = kinesisClient.listStreams(listStreamsRequest)
      streamNames.addAll(listStreamsResult.getStreamNames)
    }
    streamNames
  }

  def deleteInputStream(config: KinesisConnectorConfiguration) {
    val kinesisClient = new AmazonKinesisClient(config.AWS_CREDENTIALS_PROVIDER)
    kinesisClient.setRegion(RegionUtils.getRegion(config.REGION_NAME))
    if (config.KINESIS_ENDPOINT != null) {
      kinesisClient.setEndpoint(config.KINESIS_ENDPOINT)
    }
    deleteStream(kinesisClient, config.KINESIS_INPUT_STREAM)
  }

  def deleteOutputStream(config: KinesisConnectorConfiguration) {
    val kinesisClient = new AmazonKinesisClient(config.AWS_CREDENTIALS_PROVIDER)
    kinesisClient.setRegion(RegionUtils.getRegion(config.REGION_NAME))
    if (config.KINESIS_ENDPOINT != null) {
      kinesisClient.setEndpoint(config.KINESIS_ENDPOINT)
    }
    deleteStream(kinesisClient, config.KINESIS_OUTPUT_STREAM)
  }

  def deleteStream(kinesisClient: AmazonKinesisClient, streamName: String) {
    if (streamExists(kinesisClient, streamName)) {
      val deleteStreamRequest = new DeleteStreamRequest()
      deleteStreamRequest.setStreamName(streamName)
      kinesisClient.deleteStream(deleteStreamRequest)
      LOG.info("Deleting stream " + streamName)
    } else {
      LOG.warn("Stream " + streamName + " does not exist")
    }
  }
}
