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

import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.data.runtime.DynamicTransactionExecutorFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.transaction.TransactionExecutorFactory;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.runtime.schedule.trigger.PartitionTrigger;
import co.cask.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.schedule.constraint.Constraint;
import co.cask.cdap.proto.Notification;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.WorkflowId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionSystemClient;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * Tests for {@link JobQueueDataset}.
 */
public class JobQueueDatasetTest extends AppFabricTestBase {

  @Test
  public void checkDatasetType() throws DatasetManagementException {
    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    Assert.assertTrue(dsFramework.hasType(NamespaceId.SYSTEM.datasetType(JobQueueDataset.class.getName())));
  }

  @Test
  public void testMessageId() throws Exception {
    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    TransactionSystemClient txClient = getInjector().getInstance(TransactionSystemClient.class);
    TransactionExecutorFactory txExecutorFactory = new DynamicTransactionExecutorFactory(txClient);
    final JobQueueDataset jobQueue = dsFramework.getDataset(Schedulers.JOB_QUEUE_DATASET_ID,
                                                         new HashMap<String, String>(), null);
    Assert.assertNotNull(jobQueue);
    TransactionExecutor txExecutor =
      txExecutorFactory.createExecutor(Collections.singleton((TransactionAware) jobQueue));

    final String topic1 = "topic1";
    final String topic2 = "topic2";
    final String messageIdToPut = "messageIdToPut";

    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // without first setting the message Id, a get will return null
        Assert.assertNull(jobQueue.getMessageId(topic1));

        // test set and get
        jobQueue.setMessageId(topic1, messageIdToPut);
        Assert.assertEquals(messageIdToPut, jobQueue.getMessageId(topic1));

        // the message id for a different topic should still be null
        Assert.assertNull(jobQueue.getMessageId(topic2));
      }
    });

    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // the message Id should be retrievable across transactions
        Assert.assertEquals(messageIdToPut, jobQueue.getMessageId(topic1));
        Assert.assertNull(jobQueue.getMessageId(topic2));
      }
    });
  }

  @Test
  public void testJobQueue() throws Exception {
    DatasetFramework dsFramework = getInjector().getInstance(DatasetFramework.class);
    TransactionSystemClient txClient = getInjector().getInstance(TransactionSystemClient.class);
    TransactionExecutorFactory txExecutorFactory = new DynamicTransactionExecutorFactory(txClient);
    final JobQueueDataset jobQueue = dsFramework.getDataset(Schedulers.JOB_QUEUE_DATASET_ID,
                                                         new HashMap<String, String>(), null);
    Assert.assertNotNull(jobQueue);
    TransactionExecutor txExecutor =
      txExecutorFactory.createExecutor(Collections.singleton((TransactionAware) jobQueue));


    NamespaceId testNamespace = new NamespaceId("jobQueueTest");
    ApplicationId appId = testNamespace.app("app1");
    WorkflowId workflowId = appId.workflow("wf1");
    DatasetId datasetId = testNamespace.dataset("pfs1");
    final ProgramSchedule sched1 = new ProgramSchedule("sched1", "one partition schedule", workflowId,
                                                       ImmutableMap.of("prop3", "abc"),
                                                       new PartitionTrigger(datasetId, 1),
                                                       ImmutableList.<Constraint>of());
    final ProgramSchedule sched2 = new ProgramSchedule("sched2", "time schedule", workflowId,
                                                       ImmutableMap.of("prop3", "abc"),
                                                       new TimeTrigger("* * * * *"),
                                                       ImmutableList.<Constraint>of());

    final Job sched1Job = new SimpleJob(sched1, System.currentTimeMillis(), Lists.<Notification>newArrayList(),
                                        Job.JobState.PENDING_TRIGGER);
    final Job sched2Job = new SimpleJob(sched2, System.currentTimeMillis(), Lists.<Notification>newArrayList(),
                                        Job.JobState.PENDING_TRIGGER);

    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        // should be 0 jobs in the JobQueue to begin with
        Assert.assertEquals(0, toList(jobQueue.getAllJobs()).size());
        Assert.assertEquals(0, jobQueue.getJobsForSchedule(sched1.getScheduleId()).size());
        Assert.assertEquals(0, jobQueue.getJobsForSchedule(sched2.getScheduleId()).size());

        // put a job for sched1, and check that it is in 'getAllJobs' and 'getJobsForSchedule'
        jobQueue.put(sched1Job);

        Assert.assertEquals(ImmutableList.of(sched1Job), toList(jobQueue.getAllJobs()));
        Assert.assertEquals(ImmutableList.of(sched1Job), jobQueue.getJobsForSchedule(sched1.getScheduleId()));

        // the job for sched1 should not be in 'getJobsForSchedule' for sched2
        Assert.assertEquals(0, jobQueue.getJobsForSchedule(sched2.getScheduleId()).size());

        // put a job for sched2 and verify that it is also in the returned list
        jobQueue.put(sched2Job);

        Assert.assertEquals(ImmutableList.of(sched1Job, sched2Job), toList(jobQueue.getAllJobs()));
        Assert.assertEquals(ImmutableList.of(sched2Job), jobQueue.getJobsForSchedule(sched2.getScheduleId()));

        // delete job for sched1 and assert that it is no longer in 'getAllJobs'
        jobQueue.deleteJobs(sched1.getScheduleId());
      }
    });

    // doing the last assertion in a separate transaction to workaround CDAP-3511
    txExecutor.execute(new TransactionExecutor.Subroutine() {
      @Override
      public void apply() throws Exception {
        Assert.assertEquals(ImmutableList.of(sched2Job), toList(jobQueue.getAllJobs()));
        Assert.assertEquals(0, jobQueue.getJobsForSchedule(sched1.getScheduleId()).size());
        Assert.assertEquals(ImmutableList.of(sched2Job), jobQueue.getJobsForSchedule(sched2.getScheduleId()));
      }
    });
  }

  private List<Job> toList(CloseableIterator<Job> jobIter) {
    try {
      List<Job> jobList = new ArrayList<>();
      while (jobIter.hasNext()) {
        jobList.add(jobIter.next());
      }
      return jobList;
    } finally {
      jobIter.close();
    }
  }
}
