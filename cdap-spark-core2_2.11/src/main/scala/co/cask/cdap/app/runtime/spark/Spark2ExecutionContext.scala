/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.app.runtime.spark

import java.io._
import java.net.URI
import java.util
import java.util.concurrent.TimeUnit

import co.cask.cdap.api.data.batch.{BatchWritable, Split}
import co.cask.cdap.api.dataset.Dataset
import co.cask.cdap.api.spark.SparkExecutionContext
import co.cask.cdap.data2.metadata.lineage.AccessType
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler._
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.tephra.TransactionAware
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Default implementation of [[co.cask.cdap.api.spark.SparkExecutionContext]].
  *
  * @param runtimeContext provides access to CDAP internal services
  * @param localizeResources a Map from name to local file that the user requested to localize during the
  *                          beforeSubmit call.
  */
class Spark2ExecutionContext(override val runtimeContext: SparkRuntimeContext,
                             override val localizeResources: util.Map[String, File])
  extends SparkExecutionContext with DefaultSparkExecutionContext with AutoCloseable {

  import Spark2ExecutionContext._

  // Start the Spark TX service
  sparkTxService.startAndWait()

  // Attach a listener to the SparkContextCache, which will in turn listening to events from SparkContext.
  SparkRuntimeEnv.addSparkListener(new SparkListener {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) = applicationEndLatch.countDown

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      val jobId = Integer.valueOf(jobStart.jobId)
      val stageIds = jobStart.stageInfos.map(info => info.stageId: Integer).toSet
      val sparkTransaction = Option(jobStart.properties.getProperty(SparkTransactional.ACTIVE_TRANSACTION_KEY))
        .flatMap(key => if (key.isEmpty) None else Option(transactional.getTransactionInfo(key)))

      sparkTransaction.fold({
        LOG.debug("Spark program={}, runId={}, jobId={} starts without transaction",
          runtimeContext.getProgram.getId, getRunId, jobId)
        sparkTxService.jobStarted(jobId, stageIds)
      })(info => {
        LOG.debug("Spark program={}, runId={}, jobId={} starts with auto-commit={} on transaction {}",
          runtimeContext.getProgram.getId, getRunId, jobId,
          info.commitOnJobEnded().toString, info.getTransaction)
        sparkTxService.jobStarted(jobId, stageIds, info)
        info.onJobStarted()
      })
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      sparkTxService.jobEnded(jobEnd.jobId, jobEnd.jobResult == JobSucceeded)
    }
  })

  override def fromDataset[K: ClassTag, V: ClassTag](sc: SparkContext,
                                                     datasetName: String,
                                                     arguments: Map[String, String],
                                                     splits: Option[Iterable[_ <: Split]]): RDD[(K, V)] = {
    new Spark2DatasetRDD[K, V](sc, createDatasetCompute, runtimeContext.getConfiguration, getNamespace, datasetName,
      arguments, splits, getTxServiceBaseURI(sc, sparkTxService.getBaseURI))
  }

  override def fromDataset[K: ClassTag, V: ClassTag](sc: SparkContext,
                                                     namespace: String,
                                                     datasetName: String,
                                                     arguments: Map[String, String],
                                                     splits: Option[Iterable[_ <: Split]]): RDD[(K, V)] = {
    new Spark2DatasetRDD[K, V](sc, createDatasetCompute, runtimeContext.getConfiguration, namespace,
      datasetName, arguments, splits, getTxServiceBaseURI(sc, sparkTxService.getBaseURI))
  }


  /**
    * Creates a function used by [[org.apache.spark.SparkContext]] runJob for writing data to a
    * Dataset that implements [[co.cask.cdap.api.data.batch.BatchWritable]]. The returned function
    * will be executed in executor nodes.
    */
  override def createBatchWritableFunc[K, V](namespace: String,
                                             datasetName: String,
                                             arguments: Map[String, String],
                                             txServiceBaseURI: Broadcast[URI]) =
    Spark2ExecutionContext.createBatchWritableFunc(namespace, datasetName, arguments, txServiceBaseURI)

  /**
    * Save this specified rdd as a Hadoop Dataset.
    */
  override protected def saveAsNewAPIHadoopDataset[K: ClassManifest, V: ClassManifest](sc: SparkContext,
                                                                                       conf: Configuration,
                                                                                       rdd: RDD[(K, V)]): Unit = {
    rdd.saveAsNewAPIHadoopDataset(conf)
  }
}

/**
  * Companion object for holding static fields and methods.
  */
object Spark2ExecutionContext {
  private val LOG = LoggerFactory.getLogger(classOf[Spark2ExecutionContext])

  def createBatchWritableFunc[K, V](namespace: String,
                                    datasetName: String,
                                    arguments: Map[String, String],
                                    txServiceBaseURI: Broadcast[URI]) = (context: TaskContext,
                                                                         itor: Iterator[(K, V)]) => {
    val sparkTxClient = new SparkTransactionClient(txServiceBaseURI.value)
    val datasetCache = SparkRuntimeContextProvider.get().getDatasetCache
    val dataset: Dataset = datasetCache.getDataset(namespace, datasetName, arguments, true, AccessType.WRITE)

    try {
      // Creates an Option[TransactionAware] if the dataset is a TransactionAware
      val txAware = dataset match {
        case txAware: TransactionAware => Some(txAware)
        case _ => None
      }

      // Try to get the transaction for this stage. Hardcoded the timeout to 10 seconds for now
      txAware.foreach(_.startTx(sparkTxClient.getTransaction(context.stageId(), 10, TimeUnit.SECONDS)))

      // Write through BatchWritable.
      val writable = dataset.asInstanceOf[BatchWritable[K, V]]
      var records = 0
      while (itor.hasNext) {
        val pair = itor.next()
        writable.write(pair._1, pair._2)

        // Periodically calling commitTx to flush changes. Hardcoded to 1000 records for now
        if (records > 1000) {
          txAware.foreach(_.commitTx())
          records = 0
        }
        records += 1
      }

      // Flush all writes
      txAware.foreach(_.commitTx())
    } finally {
      dataset.close()
    }
  }
}
