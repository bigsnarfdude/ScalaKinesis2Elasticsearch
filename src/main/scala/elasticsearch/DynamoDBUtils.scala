import java.util.Arrays
import java.util.List

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import com.amazonaws.services.autoscaling.model.ResourceInUseException
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.model.TableStatus

/**
 * Utilities used to create and delete Amazon DynamoDB resources.
 */
//remove if not needed
import scala.collection.JavaConversions._

object DynamoDBUtils {

  private var LOG: Log = LogFactory.getLog(classOf[DynamoDBUtils])

  def createTable(client: AmazonDynamoDBClient, 
      tableName: String, 
      key: String, 
      readCapacityUnits: Long, 
      writeCapacityUnits: Long) {
    if (tableExists(client, tableName)) {
      if (tableHasCorrectSchema(client, tableName, key)) {
        waitForActive(client, tableName)
        return
      } else {
        throw new IllegalStateException("Table already exists and schema does not match")
      }
    }
    val createTableRequest = new CreateTableRequest()
    createTableRequest.setTableName(tableName)
    createTableRequest.setKeySchema(Arrays.asList(new KeySchemaElement(key, KeyType.HASH):_*))
    createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(readCapacityUnits, writeCapacityUnits))
    createTableRequest.setAttributeDefinitions(Arrays.asList(new AttributeDefinition(key, ScalarAttributeType.S):_*))
    client.createTable(createTableRequest)
    LOG.info("Table " + tableName + " created")
    waitForActive(client, tableName)
  }

  private def waitForActive(client: AmazonDynamoDBClient, tableName: String) getTableStatus(client, tableName) match {
    case DELETING => throw new IllegalStateException("Table " + tableName + " is in the DELETING state")
    case ACTIVE => 
      LOG.info("Table " + tableName + " is ACTIVE")
      return

    case _ => 
      var startTime = System.currentTimeMillis()
      var endTime = startTime + (10 * 60 * 1000)
      while (System.currentTimeMillis() < endTime) {
        try {
          Thread.sleep(10 * 1000)
        } catch {
          case e: InterruptedException => 
        }
        if (getTableStatus(client, tableName) == TableStatus.ACTIVE) {
          LOG.info("Table " + tableName + " is ACTIVE")
          return
        }
      }

  }

  private def getTableStatus(client: AmazonDynamoDBClient, tableName: String): TableStatus = {
    val describeTableRequest = new DescribeTableRequest()
    describeTableRequest.setTableName(tableName)
    val describeTableResult = client.describeTable(describeTableRequest)
    val status = describeTableResult.getTable.getTableStatus
    TableStatus.fromValue(status)
  }

  private def tableHasCorrectSchema(client: AmazonDynamoDBClient, tableName: String, key: String): Boolean = {
    val describeTableRequest = new DescribeTableRequest()
    describeTableRequest.setTableName(tableName)
    val describeTableResult = client.describeTable(describeTableRequest)
    val tableDescription = describeTableResult.getTable
    if (tableDescription.getAttributeDefinitions.size != 1) {
      LOG.error("The number of attribute definitions does not match the existing table.")
      return false
    }
    val attributeDefinition = tableDescription.getAttributeDefinitions.get(0)
    if (attributeDefinition.getAttributeName != key || 
      attributeDefinition.getAttributeType != ScalarAttributeType.S.toString) {
      LOG.error("Attribute name or type does not match existing table.")
      return false
    }
    val KSEs = tableDescription.getKeySchema
    if (KSEs.size != 1) {
      LOG.error("The number of key schema elements does not match the existing table.")
      return false
    }
    val kse = KSEs.get(0)
    if (kse.getAttributeName != key || kse.getKeyType != KeyType.HASH.toString) {
      LOG.error("The hash key does not match the existing table.")
      return false
    }
    true
  }

  private def tableExists(client: AmazonDynamoDBClient, tableName: String): Boolean = {
    val describeTableRequest = new DescribeTableRequest()
    describeTableRequest.setTableName(tableName)
    try {
      client.describeTable(describeTableRequest)
      true
    } catch {
      case e: ResourceNotFoundException => false
    }
  }

  def deleteTable(client: AmazonDynamoDBClient, tableName: String) {
    if (tableExists(client, tableName)) {
      val deleteTableRequest = new DeleteTableRequest()
      deleteTableRequest.setTableName(tableName)
      client.deleteTable(deleteTableRequest)
      LOG.info("Deleted table " + tableName)
    } else {
      LOG.warn("Table " + tableName + " does not exist")
    }
  }
}
