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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.internal.app.runtime.ProgramClassLoader;
import co.cask.cdap.internal.app.runtime.plugin.PluginClassLoaders;
import org.apache.spark.SparkConf;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ClassLoader being used in Spark execution context. It is used in driver as well as in executor node.
 * It load classes from {@link ProgramClassLoader} followed by Plugin classes and then CDAP system ClassLoader.
 *
 * @param <T> the type of SparkExecutionContext
 */
public abstract class SparkClassLoader<T> extends CombineClassLoader {

  protected final SparkRuntimeContext runtimeContext;
  private T sparkExecutionContext;

  /**
   * Creates a new SparkClassLoader with the given {@link SparkRuntimeContext}.
   */
  public SparkClassLoader(SparkRuntimeContext runtimeContext) {
    super(null, createDelegateClassLoaders(runtimeContext));
    this.runtimeContext = runtimeContext;
  }

  /**
   * Returns the program ClassLoader.
   */
  public ClassLoader getProgramClassLoader() {
    return runtimeContext.getProgram().getClassLoader();
  }

  /**
   * Returns the {@link SparkRuntimeContext}.
   */
  public SparkRuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  /**
   * Returns the SparkExecutionContext.
   *
   * @param createIfNotExists {@code true} to create a new SparkExecutionContext if one doesn't exist yet
   *                                      in the current execution context. Only the SparkMainWrapper should
   *                                      pass in {@code true}.
   */
  public synchronized T getSparkExecutionContext(boolean createIfNotExists) {
    if (sparkExecutionContext != null) {
      return sparkExecutionContext;
    }

    if (!createIfNotExists) {
      throw new IllegalStateException(
        "SparkExecutionContext does not exist. " +
          "This is caused by using SparkExecutionContext from a " +
          "closure function executing in Spark executor process. " +
          "SparkExecutionContext can only be used in Spark driver process.");
    }

    SparkConf sparkConf = new SparkConf();
    File resourcesDir = new File(sparkConf.get("spark.local.dir", System.getProperty("user.dir")));
    sparkExecutionContext =
      createSparkExecutionContext(SparkRuntimeUtils.getLocalizedResources(resourcesDir, sparkConf));
    return sparkExecutionContext;
  }

  /**
   * Create the actual SparkExecutionContext
   */
  protected abstract T createSparkExecutionContext(Map<String, File> localizedResources);

  /**
   * Creates the delegating list of ClassLoader. Used by constructor only.
   */
  private static List<ClassLoader> createDelegateClassLoaders(SparkRuntimeContext context) {
    return Arrays.asList(
      context.getProgram().getClassLoader(),
      PluginClassLoaders.createFilteredPluginsClassLoader(context.getApplicationSpecification().getPlugins(),
                                                          context.getPluginInstantiator()),
      SparkClassLoader.class.getClassLoader()
    );
  }
}

