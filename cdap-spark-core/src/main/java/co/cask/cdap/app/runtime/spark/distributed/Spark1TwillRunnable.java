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

package co.cask.cdap.app.runtime.spark.distributed;

import co.cask.cdap.app.runtime.spark.Spark1ProgramRuntimeProvider;
import co.cask.cdap.app.runtime.spark.SparkProgramRuntimeProvider;

/**
 * Spark1 Twill Runnable
 */
public class Spark1TwillRunnable extends SparkTwillRunnable {

  Spark1TwillRunnable(String name) {
    super(name);
  }

  @Override
  protected SparkProgramRuntimeProvider getRuntimeProvider() {
    return new Spark1ProgramRuntimeProvider();
  }
}
