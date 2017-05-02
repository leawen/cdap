/*
 * Copyright © 2016 Cask Data, Inc.
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
import java.util.concurrent.CountDownLatch

import co.cask.cdap.api._
import co.cask.cdap.api.app.ApplicationSpecification
import co.cask.cdap.api.data.batch.{BatchWritable, DatasetOutputCommitter, OutputFormatProvider}
import co.cask.cdap.api.data.format.FormatSpecification
import co.cask.cdap.api.dataset.Dataset
import co.cask.cdap.api.flow.flowlet.StreamEvent
import co.cask.cdap.api.messaging.MessagingContext
import co.cask.cdap.api.metrics.Metrics
import co.cask.cdap.api.plugin.PluginContext
import co.cask.cdap.api.preview.DataTracer
import co.cask.cdap.api.security.store.{SecureStore, SecureStoreData}
import co.cask.cdap.api.spark.{SparkExecutionContextBase, SparkSpecification}
import co.cask.cdap.api.stream.GenericStreamEventData
import co.cask.cdap.api.workflow.{WorkflowInfo, WorkflowToken}
import co.cask.cdap.app.runtime.spark.SparkTransactional.TransactionType
import co.cask.cdap.app.runtime.spark.preview.SparkDataTracer
import co.cask.cdap.app.runtime.spark.stream.SparkStreamInputFormat
import co.cask.cdap.common.conf.ConfigurationUtil
import co.cask.cdap.data.stream.{AbstractStreamInputFormat, StreamUtils}
import co.cask.cdap.data2.metadata.lineage.AccessType
import co.cask.cdap.internal.app.runtime.DefaultTaskLocalizationContext
import co.cask.cdap.proto.id.StreamId
import co.cask.cdap.proto.security.Action
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.MRJobConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.twill.api.RunId
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

/**
  * Base implementation of SparkExecutionContext.
  */
trait DefaultSparkExecutionContext extends SparkExecutionContextBase with AutoCloseable {

  // child should override this
  def runtimeContext: SparkRuntimeContext

  // child should override this
  def localizeResources: util.Map[String, File]

  // Import the companion object for static fields
  import DefaultSparkExecutionContext._

  private val taskLocalizationContext = new DefaultTaskLocalizationContext(localizeResources)
  private val workflowInfo = Option(runtimeContext.getWorkflowInfo)
  private val authorizationEnforcer = runtimeContext.getAuthorizationEnforcer
  private val authenticationContext = runtimeContext.getAuthenticationContext
  protected val applicationEndLatch = new CountDownLatch(1)
  protected val transactional = new SparkTransactional(runtimeContext.getTransactionSystemClient,
                                                       runtimeContext.getDatasetCache,
                                                       runtimeContext.getRetryStrategy)
  protected val sparkTxService = new SparkTransactionService(runtimeContext.getTransactionSystemClient,
                                                             runtimeContext.getHostname,
                                                             runtimeContext.getProgramName)

  override def close() = {
    try {
      // If there is a SparkContext, wait for the ApplicationEnd callback
      // This make sure all jobs' transactions are committed/invalidated
      SparkRuntimeEnvBase.stop().foreach(sc => applicationEndLatch.await())
    } finally {
      sparkTxService.stopAndWait()
    }
  }

  override def getApplicationSpecification: ApplicationSpecification = runtimeContext.getApplicationSpecification

  override def getClusterName: String = runtimeContext.getClusterName

  override def getRuntimeArguments: util.Map[String, String] = runtimeContext.getRuntimeArguments

  override def getRunId: RunId = runtimeContext.getRunId

  override def getNamespace: String = runtimeContext.getProgram.getNamespaceId

  override def getAdmin: Admin = runtimeContext.getAdmin

  override def getSpecification: SparkSpecification = runtimeContext.getSparkSpecification

  override def getLogicalStartTime: Long = runtimeContext.getLogicalStartTime

  override def getServiceDiscoverer: ServiceDiscoverer = new SparkServiceDiscoverer(runtimeContext)

  override def getMetrics: Metrics = new SparkUserMetrics(runtimeContext)

  override def getSecureStore: SecureStore = new SparkSecureStore(runtimeContext)

  // TODO: CDAP-7807. Returns one that is serializable
  override def getMessagingContext: MessagingContext = runtimeContext

  override def getPluginContext: PluginContext = new SparkPluginContext(runtimeContext)

  override def getWorkflowToken: Option[WorkflowToken] = workflowInfo.map(_.getWorkflowToken)

  override def getWorkflowInfo: Option[WorkflowInfo] = workflowInfo

  override def getLocalizationContext: TaskLocalizationContext = taskLocalizationContext

  override def execute(runnable: TxRunnable): Unit = {
    transactional.execute(runnable)
  }

  override def execute(timeout: Int, runnable: TxRunnable): Unit = {
    transactional.execute(timeout, runnable)
  }

  override def fromStream[T: ClassTag](sc: SparkContext, streamName: String, startTime: Long, endTime: Long)
                                      (implicit decoder: StreamEvent => T): RDD[T] = {
    val rdd: RDD[(Long, StreamEvent)] = fromStream(sc, getNamespace, streamName, startTime, endTime, None)

    // Wrap the StreamEvent with a SerializableStreamEvent
    // Don't use rdd.values() as it brings in implicit object from SparkContext, which is not available in Spark 1.2
    rdd.map(t => new SerializableStreamEvent(t._2)).map(decoder)
  }

  override def fromStream[T: ClassTag](sc: SparkContext, namespace: String, streamName: String, startTime: Long,
                                       endTime: Long) (implicit decoder: StreamEvent => T): RDD[T] = {
    val rdd: RDD[(Long, StreamEvent)] = fromStream(sc, namespace, streamName, startTime, endTime, None)

    // Wrap the StreamEvent with a SerializableStreamEvent
    // Don't use rdd.values() as it brings in implicit object from SparkContext, which is not available in Spark 1.2
    rdd.map(t => new SerializableStreamEvent(t._2)).map(decoder)
  }

  override def fromStream[T: ClassTag](sc: SparkContext, streamName: String, formatSpec: FormatSpecification,
                                       startTime: Long, endTime: Long): RDD[(Long, GenericStreamEventData[T])] = {
    fromStream(sc, getNamespace, streamName, startTime, endTime, Some(formatSpec))
  }

  override def fromStream[T: ClassTag](sc: SparkContext, namespace: String, streamName: String,
                                       formatSpec: FormatSpecification, startTime: Long, endTime: Long):
  RDD[(Long, GenericStreamEventData[T])] = {
    fromStream(sc, namespace, streamName, startTime, endTime, Some(formatSpec))
  }

  /**
    * Creates a [[org.apache.spark.rdd.RDD]] by reading from the given stream and time range.
    *
    * @param sc the [[org.apache.spark.SparkContext]] to use
    * @param namespace namespace of the stream
    * @param streamName name of the stream
    * @param startTime  the starting time of the stream to be read in milliseconds (inclusive);
    *                   passing in `0` means start reading from the first event available in the stream.
    * @param endTime the ending time of the streams to be read in milliseconds (exclusive);
    *                passing in `Long#MAX_VALUE` means read up to latest event available in the stream.
    * @param formatSpec if provided, it describes the format in the stream and will be used to decode stream events
    *                   to the given value type `T`
    * @return a new [[org.apache.spark.rdd.RDD]] instance that reads from the given stream.
    */
  private def fromStream[T: ClassTag](sc: SparkContext, namespace: String, streamName: String,
                                      startTime: Long, endTime: Long,
                                      formatSpec: Option[FormatSpecification]): RDD[(Long, T)] = {
    val streamId = new StreamId(namespace, streamName)

    // Clone the configuration since it's dataset specification and shouldn't affect the global hConf
    val configuration = configureStreamInput(new Configuration(runtimeContext.getConfiguration),
      streamId, startTime, endTime, formatSpec)

    val valueClass = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    val rdd = sc.newAPIHadoopRDD(configuration, classOf[SparkStreamInputFormat[LongWritable, T]],
      classOf[LongWritable], valueClass)
    recordStreamUsage(streamId)
    // check if user has READ permission on the stream to make sure we fail early. this is done after we record stream
    // usage since we want to record the intent
    authorizationEnforcer.enforce(streamId, authenticationContext.getPrincipal, Action.READ)
    rdd.map(t => (t._1.get(), t._2))
  }

  override def saveAsDataset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], datasetName: String,
                                                       arguments: Map[String, String]): Unit = {
    saveAsDataset(rdd, getNamespace, datasetName, arguments)
  }

  override def saveAsDataset[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)], namespace: String, datasetName: String,
                                                       arguments: Map[String, String]): Unit = {
    transactional.execute(new SparkTxRunnable {
      override def run(context: SparkDatasetContext) = {
        val sc = rdd.sparkContext
        val dataset: Dataset = context.getDataset(namespace, datasetName, arguments, AccessType.WRITE)
        val outputCommitter = dataset match {
          case outputCommitter: DatasetOutputCommitter => Some(outputCommitter)
          case _ => None
        }

        try {
          dataset match {
            case outputFormatProvider: OutputFormatProvider =>
              val conf = new Configuration(runtimeContext.getConfiguration)

              ConfigurationUtil.setAll(outputFormatProvider.getOutputFormatConfiguration, conf)
              conf.set(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatProvider.getOutputFormatClassName)

              saveAsNewAPIHadoopDataset(sc, conf, rdd)

            case batchWritable: BatchWritable[K, V] =>
              val txServiceBaseURI = getTxServiceBaseURI(sc, sparkTxService.getBaseURI)
              sc.runJob(rdd, createBatchWritableFunc(namespace, datasetName, arguments, txServiceBaseURI))

            case _ =>
              throw new IllegalArgumentException("Dataset is neither a OutputFormatProvider nor a BatchWritable")
          }
          outputCommitter.foreach(_.onSuccess())
        } catch {
          case t: Throwable =>
            outputCommitter.foreach(_.onFailure())
            throw t
        }
      }
    }, TransactionType.IMPLICIT)
  }

  override def getDataTracer(tracerName: String): DataTracer = new SparkDataTracer(runtimeContext, tracerName)

  @throws[IOException]
  def list(namespace: String): util.Map[String, String] = {
    return runtimeContext.listSecureData(namespace)
  }

  @throws[IOException]
  def get(namespace: String, name: String): SecureStoreData = {
    return runtimeContext.getSecureData(namespace, name)
  }

  /**
    * Creates a [[org.apache.spark.broadcast.Broadcast]] for the base URI
    * of the [[co.cask.cdap.app.runtime.spark.SparkTransactionService]]
    */
  protected def getTxServiceBaseURI(sc: SparkContext, baseURI: URI): Broadcast[URI] = {
    this.synchronized {
      txServiceBaseURI match {
        case Some(uri) => uri
        case None =>
          val broadcast = sc.broadcast(baseURI)
          txServiceBaseURI = Some(broadcast)
          broadcast
      }
    }
  }

  /**
    * Creates a [[co.cask.cdap.app.runtime.spark.DatasetCompute]] for the DatasetRDD to use.
    */
  protected def createDatasetCompute: DatasetCompute = {
    new DatasetCompute {
      override def apply[T: ClassTag](namespace: String, datasetName: String,
                                      arguments: Map[String, String], f: (Dataset) => T): T = {
        val result = new Array[T](1)

        // For RDD to compute partitions, a transaction is needed in order to gain access to dataset instance.
        // It should either be using the active transaction (explicit transaction), or create a new transaction
        // but leave it open so that it will be used for all stages in same job execution and get committed when
        // the job ended.
        transactional.execute(new SparkTxRunnable {
          override def run(context: SparkDatasetContext) = {
            val dataset: Dataset = context.getDataset(namespace, datasetName, arguments, AccessType.READ)
            try {
              result(0) = f(dataset)
            } finally {
              context.releaseDataset(dataset)
            }
          }
        }, TransactionType.IMPLICIT_COMMIT_ON_JOB_END)
        result(0)
      }
    }
  }

  private def configureStreamInput(configuration: Configuration, streamId: StreamId, startTime: Long,
                                   endTime: Long, formatSpec: Option[FormatSpecification]): Configuration = {
    val streamConfig = runtimeContext.getStreamAdmin.getConfig(streamId)
    val streamPath = StreamUtils.createGenerationLocation(streamConfig.getLocation,
                                                          StreamUtils.getGeneration(streamConfig))
    AbstractStreamInputFormat.setStreamId(configuration, streamId)
    AbstractStreamInputFormat.setTTL(configuration, streamConfig.getTTL)
    AbstractStreamInputFormat.setStreamPath(configuration, streamPath.toURI)
    AbstractStreamInputFormat.setTimeRange(configuration, startTime, endTime)
    // Either use the identity decoder or use the format spec to decode
    formatSpec.fold(
      AbstractStreamInputFormat.inferDecoderClass(configuration, classOf[StreamEvent])
    )(
      spec => AbstractStreamInputFormat.setBodyFormatSpecification(configuration, spec)
    )
    configuration
  }

  private def recordStreamUsage(streamId: StreamId): Unit = {
    val oldStreamId = streamId.toId

    // Register for stream usage for the Spark program
    val oldProgramId = runtimeContext.getProgram.getId
    val owners = List(oldProgramId)
    try {
      runtimeContext.getStreamAdmin.register(owners, oldStreamId.toEntityId)
      runtimeContext.getStreamAdmin.addAccess(oldProgramId.run(getRunId.getId), oldStreamId.toEntityId,
        AccessType.READ)
    }
    catch {
      case e: Exception =>
        LOG.warn("Failed to register usage of {} -> {}", streamId, owners, e)
    }
  }

  /**
    * Save this specified rdd as a Hadoop Dataset. Spark1.2 needs special logic.
    */
  protected def saveAsNewAPIHadoopDataset[K: ClassTag, V: ClassTag](sc: SparkContext, conf: Configuration,
                                                                    rdd: RDD[(K, V)]): Unit

  /**
    * Creates a function used by [[org.apache.spark.SparkContext]] runJob for writing data to a
    * Dataset that implements [[co.cask.cdap.api.data.batch.BatchWritable]]. The returned function
    * will be executed in executor nodes. This is implemented by Spark specific contexts because metrics
    * are different between spark versions
    */
  protected def createBatchWritableFunc[K, V](namespace: String,
                                              datasetName: String,
                                              arguments: Map[String, String],
                                              txServiceBaseURI: Broadcast[URI]): (TaskContext, Iterator[(K, V)]) => Unit
}

/**
  * Companion object for holding static fields and methods.
  */
object DefaultSparkExecutionContext {
  private val LOG = LoggerFactory.getLogger(classOf[DefaultSparkExecutionContext])
  private var txServiceBaseURI: Option[Broadcast[URI]] = None
}
