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

import java.net.URI

import co.cask.cdap.api.data.batch.{BatchReadable, Split}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * A [[org.apache.spark.rdd.RDD]] for reading data from [[co.cask.cdap.api.dataset.Dataset]].
  */
class Spark2DatasetRDD[K: ClassTag, V: ClassTag](@transient sc: SparkContext,
                                                 @transient datasetCompute: DatasetCompute,
                                                 @transient hConf: Configuration,
                                                 namespace: String,
                                                 datasetName: String,
                                                 arguments: Map[String, String],
                                                 @transient splits: Option[Iterable[_ <: Split]],
                                                 txServiceBaseURI: Broadcast[URI])
  extends DatasetRDD[K, V](sc, datasetCompute, hConf, namespace, datasetName,
                                   arguments, splits, txServiceBaseURI) {

  override def getInputFormatClass(inputFormatClassName: String): Class[InputFormat[K, V]] = {
    Spark2ClassLoader.findFromContext()
      .loadClass(inputFormatClassName)
      .asInstanceOf[Class[InputFormat[K, V]]]
  }

  override def getBatchReadableRDD(batchReadable: BatchReadable[K, V]): RDD[(K, V)] = {
    new Spark2BatchReadableRDD[K, V](sc, batchReadable, namespace, datasetName, arguments, splits, txServiceBaseURI)
  }
}
