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

import co.cask.cdap.api.spark.{JavaSparkExecutionContext, SparkExecutionContext}

/**
  * Implementation of [[co.cask.cdap.api.spark.JavaSparkExecutionContext]] that simply delegates all calls to
  * a [[co.cask.cdap.api.spark.SparkExecutionContext]].
  */
@SerialVersionUID(0L)
class JavaSpark2ExecutionContext(override val sec: SparkExecutionContext)
  extends JavaSparkExecutionContext with DefaultJavaSparkExecutionContext with Serializable {

}
