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

package co.cask.cdap.scheduler;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.data2.transaction.TxCallable;
import co.cask.cdap.internal.app.runtime.schedule.ProgramSchedule;
import co.cask.cdap.internal.app.runtime.schedule.ScheduleTaskRunner;
import co.cask.cdap.internal.app.runtime.schedule.queue.Job;
import co.cask.cdap.internal.app.runtime.schedule.queue.JobQueueDataset;
import co.cask.cdap.internal.app.runtime.schedule.store.Schedulers;
import co.cask.cdap.internal.app.services.ProgramLifecycleService;
import co.cask.cdap.internal.app.services.PropertiesResolver;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Polls the JobQueue, checks the jobs for constraint satisfaction, and launches them.
 */
class ConstraintCheckerService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(ConstraintCheckerService.class);

  private final List<ConstraintCheckerThread> constraintCheckerThreads;
  private final Transactional transactional;
  private final DatasetFramework datasetFramework;
  private final Store store;
  private final ProgramLifecycleService lifecycleService;
  private final PropertiesResolver propertiesResolver;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final CConfiguration cConf;
  private ScheduleTaskRunner taskRunner;
  private ListeningExecutorService taskExecutorService;

  @Inject
  ConstraintCheckerService(Store store,
                           ProgramLifecycleService lifecycleService, PropertiesResolver propertiesResolver,
                           NamespaceQueryAdmin namespaceQueryAdmin,
                           CConfiguration cConf,
                           DatasetFramework datasetFramework,
                           TransactionSystemClient txClient) {
    this.store = store;
    this.lifecycleService = lifecycleService;
    this.propertiesResolver = propertiesResolver;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.cConf = cConf;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(
        new SystemDatasetInstantiator(datasetFramework), txClient,
        NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.datasetFramework = datasetFramework;
    this.constraintCheckerThreads = new ArrayList<>();
  }

  @Override
  protected void startUp() throws Exception {
    taskExecutorService = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("constraint-checker-task")));
    taskRunner = new ScheduleTaskRunner(store, lifecycleService, propertiesResolver,
                                        taskExecutorService, namespaceQueryAdmin, cConf);
  }

  @Override
  protected void run() {
    LOG.info("Start running ConstraintCheckerService");
    if (!isRunning()) {
      return;
    }

    // currently, only have one constraint checker thread (job queue is not partitioned)
    constraintCheckerThreads.add(new ConstraintCheckerThread());

    for (ConstraintCheckerThread thread : constraintCheckerThreads) {
      thread.start();
    }

    for (ConstraintCheckerThread thread : constraintCheckerThreads) {
      Uninterruptibles.joinUninterruptibly(thread);
    }
  }

  @Override
  protected void triggerShutdown() {
    LOG.info("Stopping ConstraintCheckerService.");
    for (ConstraintCheckerThread thread : constraintCheckerThreads) {
      thread.interrupt();
    }

    LOG.info("ConstraintCheckerService stopped.");
  }

  @Override
  protected void shutDown() throws Exception {
    if (taskExecutorService != null) {
      taskExecutorService.shutdownNow();
    }
  }

  private class ConstraintCheckerThread extends Thread {
    private final RetryStrategy scheduleStrategy;
    private int failureCount;


    ConstraintCheckerThread() {
      super(String.format("ConstraintCheckerService-%s", "id"));
      // TODO: [CDAP-11370] Need to be configured in cdap-default.xml. Retry with delay ranging from 0.1s to 30s
      scheduleStrategy =
        co.cask.cdap.common.service.RetryStrategies.exponentialDelay(100, 30000, TimeUnit.MILLISECONDS);
    }


    @Override
    public void run() {
      while (isRunning()) {
        try {
          long sleepTime = checkJobQueue();
          // Don't sleep if sleepTime returned is 0
          if (sleepTime > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepTime);
          }
        } catch (InterruptedException e) {
          // sleep is interrupted, just exit without doing anything
        }
      }
    }

    /**
     * Check jobs in job queue for constraint satisfaction.
     *
     * @return sleep time in milliseconds before next fetch
     */
    private long checkJobQueue() {
      boolean emptyFetch = false;
      try {
        emptyFetch = Transactions.execute(transactional, new TxCallable<Boolean>() {
          @Override
          public Boolean call(DatasetContext context) throws Exception {
            return checkJobConstraints(context);
          }
        });
        failureCount = 0;
      } catch (Exception e) {
        LOG.warn("Failed to check Job constraints. Will retry in next run", e);
        failureCount++;
      }

      try {
        Transactions.execute(transactional, new TxCallable<Boolean>() {
          @Override
          public Boolean call(DatasetContext context) throws Exception {
            // run any ready jobs
            return runReadyJobs(context);
          }
        });
      } catch (Exception e) {
        LOG.warn("Failed to launch programs. Will retry in next run", e);
        failureCount++;
      }

      // If there is any failure, delay the next fetch based on the strategy
      if (failureCount > 0) {
        // Exponential strategy doesn't use the time component, so doesn't matter what we passed in as startTime
        return scheduleStrategy.nextRetry(failureCount, 0);
      }

      // Sleep for 2 seconds if there's no jobs in the queue
      return emptyFetch ? 2000L : 0L;
    }

    private boolean checkJobConstraints(DatasetContext context) throws Exception {
      boolean emptyScan = true;

      JobQueueDataset jobQueue = getJobQueue(context);
      try (CloseableIterator<Job> jobsIter = jobQueue.getAllJobs()) {
        while (jobsIter.hasNext() && isRunning()) {
          emptyScan = false;
          Job job = jobsIter.next();
          switch (job.getJobState()) {
            case PENDING_TRIGGER:
              if (isTriggerSatisfied(job)) {
                job.setJobState(Job.JobState.PENDING_CONSTRAINT);
                jobQueue.put(job);
              }
              break;
            case PENDING_CONSTRAINT:
              if (constraintsSatisfied(job)) {
                job.setJobState(Job.JobState.PENDING_LAUNCH);
                jobQueue.put(job);
              }
              break;
          }
        }
      }
      return emptyScan;
    }

    private boolean runReadyJobs(DatasetContext context) throws IOException, DatasetManagementException {
      JobQueueDataset jobQueue = getJobQueue(context);
      try (CloseableIterator<Job> allJobsIter = jobQueue.getAllJobs()) {
        while (allJobsIter.hasNext() && isRunning()) {
          Job job = allJobsIter.next();
          if (job.getJobState() == Job.JobState.PENDING_LAUNCH) {
            ProgramSchedule schedule = job.getSchedule();
            try {
              // TODO: Temporarily execute scheduled program without any checks. Need to check appSpec and scheduleSpec
              taskRunner.execute(schedule.getProgramId(), ImmutableMap.<String, String>of(),
                                 ImmutableMap.<String, String>of());
              LOG.debug("Run program {} in schedule", schedule.getProgramId(), schedule.getName());
              jobQueue.deleteJob(schedule.getScheduleId(), job.getCreationTime());
              return true;
            } catch (Exception e) {
              LOG.warn("Failed to run program {} in schedule {}. Skip running this program.",
                       schedule.getProgramId(), schedule.getName(), e);
            }
          }
        }
      }
      return false;
    }

    private boolean isTriggerSatisfied(Job job) {
      // TODO: implement trigger checking
      return true;
    }

    private boolean constraintsSatisfied(Job job) {
      // TODO: implement constraint checking
      return true;
    }

  }

  private JobQueueDataset getJobQueue(DatasetContext context) throws IOException, DatasetManagementException {
    return DatasetsUtil.getOrCreateDataset(context, datasetFramework, Schedulers.JOB_QUEUE_DATASET_ID,
                                           JobQueueDataset.class.getName(), DatasetProperties.EMPTY);
  }

}
