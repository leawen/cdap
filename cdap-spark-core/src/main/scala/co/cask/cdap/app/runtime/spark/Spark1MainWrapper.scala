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

import co.cask.cdap.api.common.RuntimeArguments
import co.cask.cdap.api.spark.{JavaSparkMain, SparkExecutionContext, SparkMain}

import scala.util.{Failure, Success, Try}

/**
  * The main class that get submitted to Spark for execution of Spark program in CDAP.
  * The first command line argument to this class is the name of the user's Spark program class.
  */
object Spark1MainWrapper extends SparkMainWrapper[SparkExecutionContext] {

  def main(args: Array[String]): Unit = {
    run(args)
  }

  override def getSparkClassLoader(): SparkClassLoader[SparkExecutionContext] = {
    // Find the SparkRuntimeContext from the classloader. Create a new one if not found (for Spark 1.2)
    Try(Spark1ClassLoader.findFromContext()) match {
      case Success(classLoader) => classLoader
      case Failure(exception) =>
        val classLoader = Spark1ClassLoader.create()
        // For Spark 1.2 driver. No need to reset the classloader at the end since this is the driver process.
        SparkRuntimeUtils.setContextClassLoader(classLoader)
        classLoader
    }
  }

  override def runSparkProgram(userSparkClass: Class[_],
                               sparkClassLoader: SparkClassLoader[SparkExecutionContext],
                               runtimeContext: SparkRuntimeContext): Unit = {
    val executionContext = sparkClassLoader.getSparkExecutionContext(true)
    try {
      val serializableExecutionContext = new SerializableSpark1ExecutionContext(executionContext)
      userSparkClass match {
        // SparkMain
        case cls if classOf[SparkMain].isAssignableFrom(cls) =>
          cls.asSubclass(classOf[SparkMain]).newInstance().run(serializableExecutionContext)

        // JavaSparkMain
        case cls if classOf[JavaSparkMain].isAssignableFrom(cls) =>
          cls.asSubclass(classOf[JavaSparkMain]).newInstance().run(
            new JavaSpark1ExecutionContext(serializableExecutionContext))

        // main() method
        case cls =>
          getMainMethod(cls).fold(
            throw new IllegalArgumentException(userSparkClass.getName
              + " is not a supported Spark program. It should implement either "
              + classOf[SparkMain].getName + " or " + classOf[JavaSparkMain].getName
              + " or has a main method defined")
          )(
            _.invoke(null, RuntimeArguments.toPosixArray(runtimeContext.getRuntimeArguments))
          )
      }
    } finally {
      executionContext match {
        case c: AutoCloseable => c.close
        case _ => // no-op
      }
    }
  }
}
