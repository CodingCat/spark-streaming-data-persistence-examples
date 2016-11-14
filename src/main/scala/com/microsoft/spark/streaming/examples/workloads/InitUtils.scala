package com.microsoft.spark.streaming.examples.workloads

import com.microsoft.spark.streaming.examples.arguments.EventhubsArgumentKeys
import com.microsoft.spark.streaming.examples.arguments.EventhubsArgumentParser.ArgumentMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

private[workloads] object InitUtils {

  /**
   * In Spark 2.0.x, SparkConf must be initialized through EventhubsUtil so that required
   * data structures internal to Azure Eventhubs Client get registered with the Kryo Serializer.
   */
  val sparkConfiguration : SparkConf = EventHubsUtils.initializeSparkStreamingConfigurations

  sparkConfiguration.setAppName(this.getClass.getSimpleName)
  sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.enable", "true")
  sparkConfiguration.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
  sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
  sparkConfiguration.set("spark.streaming.stopGracefullyOnShutdown", "true")

  def createEventHubParameters(inputOptions: ArgumentMap): Map[String, String] = {
    Map[String, String](
      "eventhubs.namespace" -> inputOptions(Symbol(EventhubsArgumentKeys.EventhubsNamespace)).asInstanceOf[String],
      "eventhubs.name" -> inputOptions(Symbol(EventhubsArgumentKeys.EventhubsName)).asInstanceOf[String],
      "eventhubs.policyname" -> inputOptions(Symbol(EventhubsArgumentKeys.PolicyName)).asInstanceOf[String],
      "eventhubs.policykey" -> inputOptions(Symbol(EventhubsArgumentKeys.PolicyKey)).asInstanceOf[String],
      "eventhubs.consumergroup" -> inputOptions(Symbol(EventhubsArgumentKeys.ConsumerGroup)).asInstanceOf[String],
      "eventhubs.partition.count" -> inputOptions(Symbol(EventhubsArgumentKeys.PartitionCount))
        .asInstanceOf[Int].toString,
      "eventhubs.checkpoint.interval" -> inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds))
        .asInstanceOf[Int].toString,
      "eventhubs.checkpoint.dir" -> inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).asInstanceOf[String]
    )
  }

  def createNewStreamingContext(
       inputOptions: ArgumentMap,
       userProvidedSparkContext: Option[SparkContext]): StreamingContext = {

    val sparkContext = userProvidedSparkContext.getOrElse(new SparkContext(sparkConfiguration))

    val ssc = new StreamingContext(sparkContext,
      Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int]))
    ssc.checkpoint(inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).toString)
    ssc
  }

  def createEventHubsWindowedStream(
      ssc: StreamingContext,
      eventHubsParams: Map[String, String],
      windowSize: Int): DStream[Array[Byte]] = {
    val eventHubsStream = EventHubsUtils.createUnionStream(ssc, eventHubsParams)
    eventHubsStream.window(Seconds(windowSize))
  }
}
