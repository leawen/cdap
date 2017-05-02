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

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._

import scala.collection.JavaConversions._

/**
  * Contains methods that deal with SparkListener, which is different in Spark1 and Spark2.
  * This object must be named exactly the same in all spark-core modules and must have the same
  * method signatures.
  */
object SparkRuntimeEnv extends SparkRuntimeEnvTrait {
  import SparkRuntimeEnvBase._

  /**
    * Adds a [[org.apache.spark.scheduler.SparkListener]]. The given listener will be added to
    * [[org.apache.spark.SparkContext]] when it becomes available.
    */
  def addSparkListener(listener: SparkListener): Unit = sparkListeners.add(listener)

  /**
    * Sets the [[org.apache.spark.SparkContext]] for the execution.
    */
  def setContext(context: SparkContext): Unit = {
    this.synchronized {
      if (stopped) {
        context.stop()
        throw new IllegalStateException("Spark program is already stopped")
      }

      if (sparkContext.isDefined) {
        throw new IllegalStateException("SparkContext was already created")
      }

      sparkContext = Some(context)
      context.addSparkListener(new DelegatingSparkListener)
    }
  }

  /**
    * A delegating [[org.apache.spark.scheduler.SparkListener]] that simply dispatch all callbacks to a list
    * of [[org.apache.spark.scheduler.SparkListener]].
    */
  private class DelegatingSparkListener extends SparkListener {

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) =
      sparkListeners.foreach(_.onStageCompleted(stageCompleted))

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) =
      sparkListeners.foreach(_.onStageSubmitted(stageSubmitted))

    override def onTaskStart(taskStart: SparkListenerTaskStart) =
      sparkListeners.foreach(_.onTaskStart(taskStart))

    override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) =
      sparkListeners.foreach(_.onTaskGettingResult(taskGettingResult))

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) =
      sparkListeners.foreach(_.onTaskEnd(taskEnd))

    override def onJobStart(jobStart: SparkListenerJobStart) =
      sparkListeners.foreach(_.onJobStart(jobStart))

    override def onJobEnd(jobEnd: SparkListenerJobEnd) =
      sparkListeners.foreach(_.onJobEnd(jobEnd))

    override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) =
      sparkListeners.foreach(_.onEnvironmentUpdate(environmentUpdate))

    override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) =
      sparkListeners.foreach(_.onBlockManagerAdded(blockManagerAdded))

    override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) =
      sparkListeners.foreach(_.onBlockManagerRemoved(blockManagerRemoved))

    override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) =
      sparkListeners.foreach(_.onUnpersistRDD(unpersistRDD))

    override def onApplicationStart(applicationStart: SparkListenerApplicationStart) =
      sparkListeners.foreach(_.onApplicationStart(applicationStart))

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) =
      sparkListeners.foreach(_.onApplicationEnd(applicationEnd))

    override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) =
      sparkListeners.foreach(_.onExecutorMetricsUpdate(executorMetricsUpdate))

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) =
      sparkListeners.foreach(_.onExecutorAdded(executorAdded))

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) =
      sparkListeners.foreach(_.onExecutorRemoved(executorRemoved))

    override def onOtherEvent(event: SparkListenerEvent) =
      sparkListeners.foreach(_.onOtherEvent(event))
  }
}
