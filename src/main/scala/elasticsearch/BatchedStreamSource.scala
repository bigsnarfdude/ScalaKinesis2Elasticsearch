import java.io.BufferedReader
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.ObjectOutputStream
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.List
import java.util.UUID

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.model.PutRecordRequest

/**
 * This class is a data source for supplying input to the Amazon Kinesis stream. It reads lines from the
 * input file specified in the constructor and batches up records before emitting them.
 */
import BatchedStreamSource._
//remove if not needed
import scala.collection.JavaConversions._
import resource._ //use scala-arm from http://jsuereth.com/scala-arm/

object BatchedStreamSource {

  private var LOG: Log = LogFactory.getLog(classOf[BatchedStreamSource])

  private var NUM_BYTES_PER_PUT_REQUEST: Int = 50000
}

class BatchedStreamSource(config: KinesisConnectorConfiguration, inputFile: String, loopOverStreamSource: Boolean)
    extends StreamSource(config, inputFile, loopOverStreamSource) {

  var buffer: List[KinesisMessageModel] = new ArrayList[KinesisMessageModel]()

  def this(config: KinesisConnectorConfiguration, inputFile: String) {
    this(config, inputFile, false)
  }

  protected override def processInputStream(inputStream: InputStream, iteration: Int) {
    for (br <- managed(new BufferedReader(new InputStreamReader(inputStream)))){
      var line: String = null
      var lines = 0
      while ((line = br.readLine()) != null) {
        val kinesisMessageModel = objectMapper.readValue(line, classOf[KinesisMessageModel])
        buffer.add(kinesisMessageModel)
        if (numBytesInBuffer() > NUM_BYTES_PER_PUT_REQUEST) {
          val lastRecord = buffer.remove(buffer.size - 1)
          flushBuffer()
          buffer.add(lastRecord)
        }
        lines += 1
      }
      if (!buffer.isEmpty) {
        flushBuffer()
      }
      LOG.info("Added " + lines + " records to stream source.")
    }
  }

  private def bufferToBytes(): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(buffer)
    bos.toByteArray()
  }

  private def numBytesInBuffer(): Int = bufferToBytes().length

  private def flushBuffer() {
    val putRecordRequest = new PutRecordRequest()
    putRecordRequest.setStreamName(config.KINESIS_INPUT_STREAM)
    putRecordRequest.setData(ByteBuffer.wrap(bufferToBytes()))
    putRecordRequest.setPartitionKey(String.valueOf(UUID.randomUUID()))
    kinesisClient.putRecord(putRecordRequest)
    buffer.clear()
  }
}
