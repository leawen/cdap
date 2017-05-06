/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.UserErrors;
import co.cask.cdap.internal.UserMessages;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.messaging.MessagingService;
import co.cask.cdap.messaging.client.StoreRequestBuilder;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Task publisher that sends notification for a triggered schedule.
 */
public final class ScheduleTaskPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleTaskPublisher.class);

  private static final Gson GSON = new Gson();

  private final MessagingService messagingService;
  private final TopicId topicId;
  private final Store store;
  private final PropertiesResolver propertiesResolver;
  private final RunConstraintsChecker requirementsChecker;
  public ScheduleTaskPublisher(Store store, MessagingService messagingService,
                            PropertiesResolver propertiesResolver, CConfiguration cConf) {
    this.store = store;
    this.messagingService = messagingService;
    this.propertiesResolver = propertiesResolver;
    this.requirementsChecker = new RunConstraintsChecker(store);
    this.topicId = new TopicId(NamespaceId.SYSTEM.getNamespace(), cConf.get(Constants.Scheduler.TIME_EVENT_TOPIC));
  }

  /**
   * Checks if all schedule requirements are satisfied,
   * then publish notification to trigger the corresponding schedule.
   *
   * @param programId Program Id
   * @param systemOverrides Arguments that would be supplied as system runtime arguments for the program.
   * @param userOverrides Arguments to add to the user runtime arguments for the program.
   */
  public void publishNotification(ProgramId programId, Map<String, String> systemOverrides,
                                 Map<String, String> userOverrides) throws Exception {
    Map<String, String> userArgs = Maps.newHashMap();
    Map<String, String> systemArgs = Maps.newHashMap();

    String scheduleName = systemOverrides.get(ProgramOptionConstants.SCHEDULE_NAME);
    ApplicationSpecification appSpec = store.getApplication(programId.getParent());
    if (appSpec == null) {
      throw new TaskExecutionException(String.format(UserMessages.getMessage(UserErrors.PROGRAM_NOT_FOUND), programId),
                                       false);
    }

    ScheduleSpecification spec = appSpec.getSchedules().get(scheduleName);
    if (!requirementsChecker.checkSatisfied(programId, spec.getSchedule())) {
      return;
    }

    // Schedule properties are overridden by resolved preferences
    userArgs.putAll(spec.getProperties());
    userArgs.putAll(propertiesResolver.getUserProperties(programId.toId()));
    userArgs.putAll(userOverrides);

    systemArgs.putAll(propertiesResolver.getSystemProperties(programId.toId()));
    systemArgs.putAll(systemOverrides);

    Map<String, String> properties = new HashMap<>();
    properties.put("userArgs", GSON.toJson(userArgs));
    properties.put("systemArgs", GSON.toJson(systemArgs));
    properties.put("programId", programId.toString());
    properties.put("scheduleName", scheduleName);

    Notification notification = new Notification(Notification.Type.TIME, properties);
    messagingService.publish(StoreRequestBuilder.of(topicId).addPayloads(GSON.toJson(notification)).build());
  }
}
