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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.spark.SparkExecutionContextBase;

/**
 * Provides a Spark version specific information required by Spark submitter and runner classes, such as the
 * SparkClassLoader to use and the wrapper class to use.
 *
 * @param <T> type of SparkExecutionContext stored by the SparkClassLoader
 */
public interface SparkRuntimeInfoProvider<T extends SparkExecutionContextBase> {

  /**
   * Returns a version specific SparkClassLoader
   *
   * @param sparkRuntimeContext the runtime context to use in the classloader
   * @return the version specific SparkClassLoader
   */
  SparkClassLoader<T> getClassLoader(SparkRuntimeContext sparkRuntimeContext);

  /**
   * @return the SparkMainWrapper class
   */
  Class<?> getWrapperClass();
}
