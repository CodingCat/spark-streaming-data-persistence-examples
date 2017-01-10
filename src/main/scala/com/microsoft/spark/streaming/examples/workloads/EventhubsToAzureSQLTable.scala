/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.streaming.examples.workloads

import java.sql.{Connection, DriverManager, Statement}

import com.microsoft.spark.streaming.examples.arguments.EventhubsArgumentParser._
import com.microsoft.spark.streaming.examples.arguments.{EventhubsArgumentKeys, EventhubsArgumentParser}
import com.microsoft.spark.streaming.examples.common.{EventContent, StreamStatistics, StreamUtilities}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object EventhubsToAzureSQLTable {

  def createStreamingContext(inputOptions: ArgumentMap,
                             namespace: String,
                             progressDir: String): StreamingContext = {

    val eventHubsParameters = Map[String, String](
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

    val sqlDatabaseConnectionString : String = StreamUtilities.getSqlJdbcConnectionString(
      inputOptions(Symbol(EventhubsArgumentKeys.SQLServerFQDN)).asInstanceOf[String],
      inputOptions(Symbol(EventhubsArgumentKeys.SQLDatabaseName)).asInstanceOf[String],
      inputOptions(Symbol(EventhubsArgumentKeys.DatabaseUsername)).asInstanceOf[String],
      inputOptions(Symbol(EventhubsArgumentKeys.DatabasePassword)).asInstanceOf[String])

    val sqlTableName: String = inputOptions(Symbol(EventhubsArgumentKeys.EventSQLTable)).asInstanceOf[String]

    /**
      * In Spark 2.0.x, SparkConf must be initialized through EventhubsUtil so that required
      * data structures internal to Azure Eventhubs Client get registered with the Kryo Serializer.
      */

    val sparkConfiguration : SparkConf = EventHubsUtils.initializeSparkStreamingConfigurations

    sparkConfiguration.set("spark.ssl.enabled", "true")
    sparkConfiguration.set("spark.ssl.protocol", "TLS")

    sparkConfiguration.setAppName(this.getClass.getSimpleName)
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.allowBatching", "true")
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.batchingTimeout", "60000")
    sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    sparkConfiguration.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
    sparkConfiguration.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
    sparkConfiguration.set("spark.streaming.stopGracefullyOnShutdown", "true")

    val sparkSession : SparkSession = SparkSession.builder().config(sparkConfiguration).getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext,
      Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int]))
    streamingContext.checkpoint(inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).asInstanceOf[String])

    val eventHubsStream = EventHubsUtils.createDirectStreams(streamingContext,
      namespace, progressDir,
      Map(inputOptions(Symbol(EventhubsArgumentKeys.EventhubsName)).asInstanceOf[String] ->
        eventHubsParameters))

    val eventHubsWindowedStream = eventHubsStream.
      window(Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).
        asInstanceOf[Int]))

    import com.microsoft.spark.streaming.examples.common.DataFrameExtensions._

    eventHubsWindowedStream.map(m => EventContent(new String(m.getBody)))
      .foreachRDD { rdd => {
          val sparkSession = SparkSession.builder.getOrCreate
          import sparkSession.implicits._
          rdd.toDF.insertToAzureSql(sqlDatabaseConnectionString, sqlTableName)
        }
      }

    // Count number of events received the past batch

    val batchEventCount = eventHubsWindowedStream.count()

    batchEventCount.print()

    // Count number of events received so far

    val totalEventCountDStream = eventHubsWindowedStream.map(m => (StreamStatistics.streamLengthKey, 1L))
    val totalEventCount = totalEventCountDStream.updateStateByKey[Long](StreamStatistics.streamLength)
    totalEventCount.checkpoint(Seconds(inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds))
      .asInstanceOf[Int]))

    if (inputOptions.contains(Symbol(EventhubsArgumentKeys.EventCountFolder))) {

      totalEventCount.saveAsTextFiles(inputOptions(Symbol(EventhubsArgumentKeys.EventCountFolder))
        .asInstanceOf[String])
    }

    totalEventCount.print()

    streamingContext
  }

  def main(inputArguments: Array[String]): Unit = {

    val inputOptions = EventhubsArgumentParser.parseArguments(Map(), inputArguments.toList)



    EventhubsArgumentParser.verifyEventhubsToSQLTableArguments(inputOptions)

    val sqlDatabaseConnectionString : String = StreamUtilities.getSqlJdbcConnectionString(
      inputOptions(Symbol(EventhubsArgumentKeys.SQLServerFQDN)).asInstanceOf[String],
      inputOptions(Symbol(EventhubsArgumentKeys.SQLDatabaseName)).asInstanceOf[String],
      inputOptions(Symbol(EventhubsArgumentKeys.DatabaseUsername)).asInstanceOf[String],
      inputOptions(Symbol(EventhubsArgumentKeys.DatabasePassword)).asInstanceOf[String])

    val sqlTableName: String = inputOptions(Symbol(EventhubsArgumentKeys.EventSQLTable)).asInstanceOf[String]

    val sqlDriverConnection: Connection =  DriverManager.getConnection(sqlDatabaseConnectionString)

    sqlDriverConnection.setAutoCommit(false)
    val sqlDriverStatement: Statement = sqlDriverConnection.createStatement()
    sqlDriverStatement.addBatch(f"IF NOT EXISTS(SELECT * FROM sys.objects WHERE object_id" +
      f" = OBJECT_ID(N'[dbo].[$sqlTableName]') AND type in (N'U'))" +
      f"\nCREATE TABLE $sqlTableName(EventDetails NVARCHAR(128) NOT NULL)")
    sqlDriverStatement.addBatch(f"IF IndexProperty(Object_Id('$sqlTableName'), 'IX_EventDetails', 'IndexId') IS NULL" +
      f"\nCREATE CLUSTERED INDEX IX_EventDetails ON $sqlTableName(EventDetails)")
    sqlDriverStatement.executeBatch()
    sqlDriverConnection.commit()

    sqlDriverConnection.close()

    //Create or recreate streaming context

    val streamingContext = createStreamingContext(inputOptions, "nanzhu-hdinsight-eastasia",
      "hdfs://mycluster/test_sql_ssl")

    streamingContext.start()

    if (inputOptions.contains(Symbol(EventhubsArgumentKeys.TimeoutInMinutes))) {

      streamingContext.awaitTerminationOrTimeout(
        inputOptions(Symbol(EventhubsArgumentKeys.TimeoutInMinutes)).asInstanceOf[Long] * 60 * 1000)
    }
    else {

      streamingContext.awaitTermination()
    }
  }
}
