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

import co.cask.cdap.api.spark.SparkExecutionContext;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.internal.app.runtime.ProgramClassLoader;
import com.google.common.base.Preconditions;

import java.io.File;
import java.util.Map;

/**
 * ClassLoader being used in Spark execution context. It is used in driver as well as in executor node.
 * It load classes from {@link ProgramClassLoader} followed by Plugin classes and then CDAP system ClassLoader.
 */
public class Spark1ClassLoader extends SparkClassLoader<SparkExecutionContext> {

  /**
   * Finds the Spark1ClassLoader from the context ClassLoader hierarchy.
   *
   * @return the Spark1ClassLoader found
   * @throws IllegalStateException if no SparkClassLoader was found
   */
  public static Spark1ClassLoader findFromContext() {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    Spark1ClassLoader sparkClassLoader = ClassLoaders.find(contextClassLoader,
                                                           Spark1ClassLoader.class);
    // Should find the Spark ClassLoader
    Preconditions.checkState(sparkClassLoader != null, "Cannot find Spark1ClassLoader from context ClassLoader %s",
                             contextClassLoader);
    return sparkClassLoader;
  }

  /**
   * Creates a new SparkClassLoader from the execution context. It should only be called in distributed mode.
   */
  public static Spark1ClassLoader create() {
    return new Spark1ClassLoader(SparkRuntimeContextProvider.get());
  }

  /**
   * Creates a new SparkClassLoader with the given {@link SparkRuntimeContext}.
   */
  public Spark1ClassLoader(SparkRuntimeContext runtimeContext) {
    super(runtimeContext);
  }

  @Override
  protected SparkExecutionContext createSparkExecutionContext(Map<String, File> localizedResources) {
    return new Spark1ExecutionContext(runtimeContext, localizedResources);
  }
}
