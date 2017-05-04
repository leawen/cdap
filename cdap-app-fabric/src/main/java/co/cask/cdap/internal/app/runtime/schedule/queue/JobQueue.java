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

package co.cask.cdap.internal.app.runtime.schedule.queue;

import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.proto.id.ScheduleId;

import java.util.List;

/**
 * Responsible for keeping track of {@link Job}s, which correspond to schedules that have been triggered,
 * but not yet executed.
 */
interface JobQueue {

  /**
   * Returns a list of Jobs associated with the given schedule Id.
   */
  List<Job> getJobsForSchedule(ScheduleId scheduleId);

  /**
   * Creates a new Job in the queue or updates an existing Job.
   *
   * @param job the new job to put.
   */
  void put(Job job);

  /**
   * Deletes all jobs associated with the given schedule Id.
   *
   * @param scheduleId the scheduledId for which to delete
   */
  void deleteJobs(ScheduleId scheduleId);

  /**
   * Deletes the job associated with the given schedule Id and timestamp.
   *
   * @param scheduleId the scheduledId for which to delete
   * @param timestamp the timestamp for which to delete
   */
  void deleteJob(ScheduleId scheduleId, long timestamp);

  /**
   * @return A {@link CloseableIterator} over all the jobs in the JobQueue.
   */
  CloseableIterator<Job> getAllJobs();

  /**
   * Gets the messageId that was previously set for a given topic.
   */
  String getMessageId(String topic);

  /**
   * Sets a messageId to be associated with a given topic.
   */
  void setMessageId(String topic, String messageId);

}
