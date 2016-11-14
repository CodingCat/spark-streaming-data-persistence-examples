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

import com.microsoft.spark.streaming.examples.arguments.EventhubsArgumentParser._
import com.microsoft.spark.streaming.examples.arguments.{EventhubsArgumentKeys, EventhubsArgumentParser}
import com.microsoft.spark.streaming.examples.common.{EventContent, StreamStatistics}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.eventhubs.EventHubsUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object EventhubsToHiveTable {

  def createStreamingContext(inputOptions: ArgumentMap): StreamingContext = {

    val eventHubsParameters = InitUtils.createEventHubParameters(inputOptions)
    val hiveTableName: String = inputOptions(Symbol(EventhubsArgumentKeys.EventHiveTable)).
      asInstanceOf[String]
    /**
     * Table needs to be explicitly created to match the Parquet format in which the data is stored
     * by default by Spark. If not explicitly created the Hive table cannot be used from Hive and
     * can only be used from inside Spark.
     */
    val hiveTableDDL: String = f"CREATE TABLE IF NOT EXISTS $hiveTableName (EventContent string)" +
      f" STORED AS PARQUET"
    val sparkSession = SparkSession.builder.enableHiveSupport.getOrCreate
    val broadcastSparkSession = sparkSession.sparkContext.broadcast(sparkSession)

    val streamingContext = InitUtils.createNewStreamingContext(inputOptions,
      Some(sparkSession.sparkContext))
    val eventHubsWindowedStream = InitUtils.createEventHubsWindowedStream(
      streamingContext,
      eventHubsParameters,
      inputOptions(Symbol(EventhubsArgumentKeys.BatchIntervalInSeconds)).asInstanceOf[Int]
    )

    sparkSession.sql(hiveTableDDL)

    /**
      * .saveAsTable does not work so insertInto is used.
      * Refer to SPARK-16803 (https://issues.apache.org/jira/browse/SPARK-16803)
      */

    eventHubsWindowedStream.map(x => EventContent(new String(x)))
      .foreachRDD(rdd => {
        val sparkSession = broadcastSparkSession.value
        import sparkSession.implicits._
        rdd.toDS.write.mode(org.apache.spark.sql.SaveMode.Append).insertInto(hiveTableName)
      })

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

    EventhubsArgumentParser.verifyEventhubsToHiveTableArguments(inputOptions)

    //Create or recreate streaming context

    val streamingContext = StreamingContext
      .getOrCreate(inputOptions(Symbol(EventhubsArgumentKeys.CheckpointDirectory)).asInstanceOf[String],
        () => createStreamingContext(inputOptions))

    streamingContext.start()

    if(inputOptions.contains(Symbol(EventhubsArgumentKeys.TimeoutInMinutes))) {

      streamingContext.awaitTerminationOrTimeout(inputOptions(Symbol(EventhubsArgumentKeys.TimeoutInMinutes))
        .asInstanceOf[Long] * 60 * 1000)
    }
    else {

      streamingContext.awaitTermination()
    }
  }
}



