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

import javax.annotation.Nullable

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext

/**
  * Methods common to both Spark versions
  */
trait SparkRuntimeEnvTrait {

  def isStopped: Boolean = SparkRuntimeEnvBase.isStopped

  def setProperty(key: String, value: String): String = SparkRuntimeEnvBase.setProperty(key, value)

  def setupSparkConf(sparkConf: SparkConf): Unit = SparkRuntimeEnvBase.setupSparkConf(sparkConf)

  def setContext(context: StreamingContext): Unit = SparkRuntimeEnvBase.setContext(context)

  def getContext: SparkContext = SparkRuntimeEnvBase.getContext

  def setLocalProperty(key: String, value: String): Boolean = SparkRuntimeEnvBase.setLocalProperty(key, value)

  @Nullable
  def getLocalProperty(key: String): String = SparkRuntimeEnvBase.getLocalProperty(key)

  def stop(thread: Option[Thread] = None): Option[SparkContext] = SparkRuntimeEnvBase.stop(thread)
}
