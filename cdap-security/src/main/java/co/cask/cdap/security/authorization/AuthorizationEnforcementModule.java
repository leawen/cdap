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

package co.cask.cdap.security.authorization;

import co.cask.cdap.common.runtime.RuntimeModule;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Scopes;

/**
 * A module that contains bindings for {@link AuthorizationEnforcer}.
 */
public class AuthorizationEnforcementModule extends RuntimeModule {

  @Override
  public Module getInMemoryModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // bind AuthorizationEnforcer to AuthorizationEnforcementService
        bind(AuthorizationEnforcer.class).to(AuthorizationEnforcementService.class).in(Scopes.SINGLETON);
      }
    };
  }

  @Override
  public Module getStandaloneModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // bind AuthorizationEnforcementService as a singleton. This binding is used while starting/stopping
        // the service itself.
        bind(AuthorizationEnforcer.class).to(DefaultAuthorizationEnforcer.class)
          .in(Scopes.SINGLETON);
      }
    };
  }

  /**
   * Used by program containers and system services (viz explore service, stream service) that need to enforce
   * authorization in distributed mode. For fetching privileges, these components are expected to proxy via a proxy
   * service, which in turn uses the authorization enforcement modules defined by #getProxyModule
   */
  @Override
  public Module getDistributedModules() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // bind AuthorizationEnforcer to AuthorizationEnforcementService
        bind(AuthorizationEnforcer.class).to(AuthorizationEnforcementService.class).in(Scopes.SINGLETON);
      }
    };
  }

  /**
   * Returns an {@link AbstractModule} containing bindings for authorization enforcement to be used in the Master.
   */
  public AbstractModule getMasterModule() {
    return new AbstractModule() {
      @Override
      protected void configure() {
        // bind AuthorizationEnforcer to AuthorizationEnforcementService
        bind(AuthorizationEnforcer.class).to(AuthorizationEnforcementService.class).in(Scopes.SINGLETON);

        // Master runs a proxy caching service for privileges for system services and program containers to fetch
        // privileges from authorization back ends.
        // The Master service acts as a proxy for system services and program containers to authorization backends
        // for fetching privileges, since they may not have access to make requests to authorization backends.
        // e.g. Apache Sentry currently does not support proxy authentication or issue delegation tokens. As a result,
        // all requests to Sentry need to be proxied via Master, which is whitelisted.
        // Hence, bind PrivilegesFetcher to a proxy implementation, that makes a proxy call to master for fetching
        // privileges
        // bind PrivilegesFetcherProxyService as a singleton. This binding is used while starting/stopping
        // the service itself.
      }
    };
  }
}
