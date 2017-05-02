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

import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillRunnable;

import java.util.Map;

/**
 * Spark1 Twill application.
 */
public class Spark2TwillApplication extends SparkTwillApplication {

  Spark2TwillApplication(Program program, Arguments runtimeArgs, SparkSpecification spec,
                         Map<String, LocalizeResource> localizeResources, EventHandler eventHandler) {
    super(program, runtimeArgs, spec, localizeResources, eventHandler);
  }

  @Override
  protected TwillRunnable getSparkTwillRunnable(String name) {
    return new Spark2TwillRunnable(name);
  }
}
