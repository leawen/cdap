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

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.store.RuntimeStore;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

/**
 * The {@link ProgramRunner} that executes Spark program.
 */
class Spark1ProgramRunner extends SparkProgramRunner {

  @Inject
  protected Spark1ProgramRunner(CConfiguration cConf, Configuration hConf, LocationFactory locationFactory,
                                TransactionSystemClient txClient, DatasetFramework datasetFramework,
                                MetricsCollectionService metricsCollectionService,
                                DiscoveryServiceClient discoveryServiceClient, StreamAdmin streamAdmin,
                                RuntimeStore runtimeStore, SecureStore secureStore,
                                SecureStoreManager secureStoreManager, AuthorizationEnforcer authorizationEnforcer,
                                AuthenticationContext authenticationContext, MessagingService messagingService) {
    super(cConf, hConf, locationFactory, txClient, datasetFramework, metricsCollectionService,
          discoveryServiceClient, streamAdmin, runtimeStore, secureStore, secureStoreManager, authorizationEnforcer,
          authenticationContext, messagingService, new Spark1RuntimeInfoProvider());
  }

}
