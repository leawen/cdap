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

import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.proto.Notification;
import com.google.common.base.Objects;

import java.util.List;

/**
 * Simple implementation of {@link Job}.
 */
public final class SimpleJob implements Job {
  private final ProgramSchedule schedule;
  private final long creationTime;
  private final List<Notification> notifications;
  private JobState jobState;

  public SimpleJob(ProgramSchedule schedule, long creationTime, List<Notification> notifications, JobState jobState) {
    this.schedule = schedule;
    this.creationTime = creationTime;
    this.notifications = notifications;
    this.jobState = jobState;
  }

  public ProgramSchedule getSchedule() {
    return schedule;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public List<Notification> getNotifications() {
    return notifications;
  }

  public JobState getJobState() {
    return jobState;
  }

  @Override
  public void setJobState(JobState jobState) {
    this.jobState = jobState;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SimpleJob that = (SimpleJob) o;

    return Objects.equal(this.schedule, that.schedule) &&
      Objects.equal(this.creationTime, that.creationTime) &&
      Objects.equal(this.notifications, that.notifications) &&
      Objects.equal(this.jobState, that.jobState);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schedule, creationTime, notifications, jobState);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("schedule", schedule)
      .add("creationTime", creationTime)
      .add("notifications", notifications)
      .add("jobState", jobState)
      .toString();
  }
}
