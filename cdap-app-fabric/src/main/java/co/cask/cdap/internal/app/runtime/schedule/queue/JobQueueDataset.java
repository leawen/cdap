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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TriggerJsonCodec;
import co.cask.cdap.internal.schedule.trigger.Trigger;
import co.cask.cdap.proto.id.ScheduleId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Dataset that stores {@link Job}s, which correspond to schedules that have been triggered, but not yet executed.
 *
 * Row Key is in the following formats:
 *   For Jobs:
 *     'J':<scheduleId>:<timestamp>
 *   For TMS MessageId:
 *     'M':<topic>
 */
public class JobQueueDataset extends AbstractDataset implements JobQueue {

  static final String EMBEDDED_TABLE_NAME = "t"; // table
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Trigger.class, new TriggerJsonCodec()).create();

  // For now, simply serialize the entire Job into one column
  private static final byte[] COL = new byte[] {'C'};
  private static final byte[] JOB_ROW_PREFIX = new byte[] {'J'};
  private static final byte[] ROW_KEY_SEPARATOR = new byte[] {':'};
  private static final byte[] MESSAGE_ID_ROW_PREFIX = new byte[] {'M'};

  private final Table table;

  JobQueueDataset(String instanceName, @EmbeddedDataset(EMBEDDED_TABLE_NAME) Table table) {
    super(instanceName, table);
    this.table = table;
  }

  @Override
  public List<Job> getJobsForSchedule(ScheduleId scheduleId) {
    byte[] keyPrefix = getRowKeyPrefix(scheduleId);
    Scanner scanner = table.scan(keyPrefix, Bytes.stopKeyForPrefix(keyPrefix));

    List<Job> jobs = new ArrayList<>();
    Row row;
    while ((row = scanner.next()) != null) {
      jobs.add(fromRow(row));
    }
    return jobs;
  }

  @Override
  public void put(Job job) {
    table.put(toPut(job));
  }

  @Override
  public void deleteJobs(ScheduleId scheduleId) {
    byte[] keyPrefix = getRowKeyPrefix(scheduleId);
    final Scanner scanner = table.scan(keyPrefix, Bytes.stopKeyForPrefix(keyPrefix));
    Row row;
    while ((row = scanner.next()) != null) {
      table.delete(row.getRow());
    }
  }

  @Override
  public void deleteJob(ScheduleId scheduleId, long timestamp) {
    table.delete(getRowKey(scheduleId, timestamp));
  }

  @Override
  public CloseableIterator<Job> getAllJobs() {
    final Scanner scanner = table.scan(null, null);

    return new CloseableIterator<Job>() {

      private Row next = scanner.next();

      @Override
      public void close() {
        scanner.close();
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public Job next() {
        Row toReturn = next;
        next = scanner.next();
        return fromRow(toReturn);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private Job fromRow(Row row) {
    String jobJsonString = Bytes.toString(row.get(COL));
    return GSON.fromJson(jobJsonString, SimpleJob.class);
  }

  private Put toPut(Job job) {
    ScheduleId scheduleId = job.getSchedule().getScheduleId();
    return new Put(getRowKey(scheduleId, job.getCreationTime()), COL, GSON.toJson(job));
  }

  private byte[] getRowKeyPrefix(ScheduleId scheduleId) {
    // TODO: use something other than #toString()?
    return Bytes.concat(JOB_ROW_PREFIX, ROW_KEY_SEPARATOR, Bytes.toBytes(scheduleId.toString()), ROW_KEY_SEPARATOR);
  }

  private byte[] getRowKey(ScheduleId scheduleId, long timestamp) {
    return Bytes.add(getRowKeyPrefix(scheduleId), Bytes.toBytes(timestamp));
  }

  @Override
  public String getMessageId(String topic) {
    Row row = table.get(getRowKey(topic));
    byte[] messageIdBytes = row.get(COL);
    return messageIdBytes == null ? null : Bytes.toString(messageIdBytes);
  }

  @Override
  public void setMessageId(String topic, String messageId) {
    table.put(getRowKey(topic), COL, Bytes.toBytes(messageId));
  }

  private byte[] getRowKey(String topic) {
    return Bytes.concat(MESSAGE_ID_ROW_PREFIX, ROW_KEY_SEPARATOR, Bytes.toBytes(topic));
  }
}
