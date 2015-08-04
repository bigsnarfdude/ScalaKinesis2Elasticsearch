import java.io.IOException
import java.io.InputStream
import java.sql.SQLException
import java.util.ArrayList
import java.util.List
import java.util.Properties

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import CloudFormationUtils
import DynamoDBUtils
import EC2Utils
import KinesisUtils
import S3Utils

import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.cloudformation.AmazonCloudFormation
import com.amazonaws.services.cloudformation.AmazonCloudFormationClient
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.ec2.AmazonEC2
import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.KinesisConnectorExecutorBase
import com.amazonaws.services.redshift.AmazonRedshiftClient
import com.amazonaws.services.s3.AmazonS3Client
import KinesisConnectorExecutor._
//remove if not needed
import scala.collection.JavaConversions._

object KinesisConnectorExecutor {

  private val LOG = LogFactory.getLog(classOf[KinesisConnectorExecutor])
  private val CREATE_KINESIS_INPUT_STREAM = "createKinesisInputStream"
  private val CREATE_KINESIS_OUTPUT_STREAM = "createKinesisOutputStream"
  private val CREATE_DYNAMODB_DATA_TABLE = "createDynamoDBDataTable"
  private val DEFAULT_CREATE_RESOURCES = false
  private val DYNAMODB_KEY = "dynamoDBKey"
  private val DYNAMODB_READ_CAPACITY_UNITS = "readCapacityUnits"
  private val DYNAMODB_WRITE_CAPACITY_UNITS = "writeCapacityUnits"
  private val DEFAULT_DYNAMODB_READ_CAPACITY_UNITS = 1l
  private val DEFAULT_DYNAMODB_WRITE_CAPACITY_UNITS = 1l
  private val EC2_ELASTICSEARCH_FILTER_NAME = "tag:type"
  private val EC2_ELASTICSEARCH_FILTER_VALUE = "elasticsearch"
  private val CREATE_STREAM_SOURCE = "createStreamSource"
  private val LOOP_OVER_STREAM_SOURCE = "loopOverStreamSource"
  private val DEFAULT_CREATE_STREAM_SOURCE = false
  private val DEFAULT_LOOP_OVER_STREAM_SOURCE = false
  private val INPUT_STREAM_FILE = "inputStreamFile"

  private def getKinesisMessageModelFields(): List[String] = {
    val fields = new ArrayList[String]()
    fields.add("userid integer not null distkey sortkey")
    fields.add("username char(8)")
    fields.add("firstname varchar(30)")
    fields.add("lastname varchar(30)")
    fields.add("city varchar(30)")
    fields.add("state char(2)")
    fields.add("email varchar(100)")
    fields.add("phone char(14)")
    fields.add("likesports boolean")
    fields.add("liketheatre boolean")
    fields.add("likeconcerts boolean")
    fields.add("likejazz boolean")
    fields.add("likeclassical boolean")
    fields.add("likeopera boolean")
    fields.add("likerock boolean")
    fields.add("likevegas boolean")
    fields.add("likebroadway boolean")
    fields.add("likemusicals boolean")
    fields
  }

  private def parseBoolean(property: String, defaultValue: Boolean, properties: Properties): Boolean = {
    java.lang.Boolean.parseBoolean(properties.getProperty(property, java.lang.Boolean.toString(defaultValue)))
  }

  private def parseLong(property: String, defaultValue: Long, properties: Properties): Long = {
    java.lang.Long.parseLong(properties.getProperty(property, java.lang.Long.toString(defaultValue)))
  }

  private def parseInt(property: String, defaultValue: Int, properties: Properties): Int = {
    java.lang.Integer.parseInt(properties.getProperty(property, java.lang.Integer.toString(defaultValue)))
  }
}

abstract class KinesisConnectorExecutor[T, U](configFile: String) extends KinesisConnectorExecutorBase[T, U] {

  protected val config = new KinesisConnectorConfiguration(properties, getAWSCredentialsProvider)
  private val properties = new Properties()
  val configStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(configFile)

  if (configStream == null) {
    val msg = "Could not find resource " + configFile + " in the classpath"
    throw new IllegalStateException(msg)
  }

  try {
    properties.load(configStream)
    configStream.close()
  } catch {
    case e: IOException => {
      val msg = "Could not load properties file " + configFile + " from classpath"
      throw new IllegalStateException(msg, e)
    }
  }

  setupAWSResources()
  setupInputStream()
  super.initialize(config)

  def getAWSCredentialsProvider(): AWSCredentialsProvider = {
    new DefaultAWSCredentialsProviderChain()
  }

  private def setupAWSResources() {
    if (parseBoolean(CREATE_KINESIS_INPUT_STREAM, DEFAULT_CREATE_RESOURCES, properties)) {
      KinesisUtils.createInputStream(config)
    }
    if (parseBoolean(CREATE_KINESIS_OUTPUT_STREAM, DEFAULT_CREATE_RESOURCES, properties)) {
      KinesisUtils.createOutputStream(config)
    }
    if (parseBoolean(CREATE_DYNAMODB_DATA_TABLE, DEFAULT_CREATE_RESOURCES, properties)) {
      val key = properties.getProperty(DYNAMODB_KEY)
      val readCapacityUnits = parseLong(DYNAMODB_READ_CAPACITY_UNITS, DEFAULT_DYNAMODB_READ_CAPACITY_UNITS, 
        properties)
      val writeCapacityUnits = parseLong(DYNAMODB_WRITE_CAPACITY_UNITS, DEFAULT_DYNAMODB_WRITE_CAPACITY_UNITS, 
        properties)
      createDynamoDBTable(key, readCapacityUnits, writeCapacityUnits)
    }
  }

  private def setupInputStream() {
    if (parseBoolean(CREATE_STREAM_SOURCE, DEFAULT_CREATE_STREAM_SOURCE, properties)) {
      val inputFile = properties.getProperty(INPUT_STREAM_FILE)
      var streamSource: StreamSource = null
      streamSource = if (config.BATCH_RECORDS_IN_PUT_REQUEST) new BatchedStreamSource(config, inputFile, 
        parseBoolean(LOOP_OVER_STREAM_SOURCE, DEFAULT_LOOP_OVER_STREAM_SOURCE, properties)) else new StreamSource(config, 
        inputFile, parseBoolean(LOOP_OVER_STREAM_SOURCE, DEFAULT_LOOP_OVER_STREAM_SOURCE, properties))
      val streamSourceThread = new Thread(streamSource)
      LOG.info("Starting stream source.")
      streamSourceThread.start()
    }
  }

  private def createDynamoDBTable(key: String, readCapacityUnits: Long, writeCapacityUnits: Long) {
    LOG.info("Creating Amazon DynamoDB table " + config.DYNAMODB_DATA_TABLE_NAME)
    val dynamodbClient = new AmazonDynamoDBClient(config.AWS_CREDENTIALS_PROVIDER)
    dynamodbClient.setEndpoint(config.DYNAMODB_ENDPOINT)
    DynamoDBUtils.createTable(dynamodbClient, config.DYNAMODB_DATA_TABLE_NAME, key, readCapacityUnits, 
      writeCapacityUnits)
  }

}
