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

import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.app.runtime.SparkCompat;
import co.cask.cdap.app.runtime.spark.distributed.DistributedSpark2ProgramRunner;
import co.cask.cdap.proto.ProgramType;

/**
 * A {@link ProgramRuntimeProvider} that provides runtime system support for {@link ProgramType#SPARK} program.
 * This class shouldn't have dependency on Spark classes.
 */
@ProgramRuntimeProvider.SupportedProgramType(types = ProgramType.SPARK, sparkCompat = SparkCompat.SPARK2_2_11)
public class Spark2ProgramRuntimeProvider extends SparkProgramRuntimeProvider {
  
  protected String getSparkProgramRunnerClassname() {
    return Spark2ProgramRunner.class.getName();
  }

  @Override
  protected String getSparkProgramRunnerClassName(Mode mode) {
    switch (mode) {
      case LOCAL:
        return Spark2ProgramRunner.class.getName();
      case DISTRIBUTED:
        return DistributedSpark2ProgramRunner.class.getName();
      default:
        throw new IllegalStateException("Unsupported Spark execution mode " + mode);
    }
  }
}
