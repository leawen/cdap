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
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.inject.Inject;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillRunner;

import java.util.Map;

/**
 * Distributed Spark1 program runner.
 */
public class DistributedSpark2ProgramRunner extends DistributedSparkProgramRunner {

  @Inject
  DistributedSpark2ProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                 TokenSecureStoreRenewer tokenSecureStoreRenewer, Impersonator impersonator) {
    super(twillRunner, hConf, cConf, tokenSecureStoreRenewer, impersonator);
  }

  @Override
  protected TwillApplication getSparkTwillApplication(Program program, Arguments userArguments,
                                                      SparkSpecification spec,
                                                      Map<String, LocalizeResource> localizeResources) {
    return new Spark2TwillApplication(program, userArguments, spec, localizeResources, eventHandler);
  }
}
