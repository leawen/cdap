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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.SparkExecutionContext;
import co.cask.cdap.data2.dataset2.DynamicDatasetCache;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionAware;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.reflect.ClassTag;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link Transactional} for managing transactions for Spark job execution.
 */
final class SparkTransactional implements Transactional {

  /**
   * Enum to represent the type of transaction.
   */
  enum TransactionType {
    /**
     * Explicit transaction, initiated by {@link Transactional#execute(TxRunnable)}
     */
    EXPLICIT,

    /**
     * Implicit transaction, initiated by
     * {@link SparkExecutionContext#saveAsDataset(RDD, String, scala.collection.immutable.Map, ClassTag, ClassTag)}
     */
    IMPLICIT,

    /**
     * Implicit transaction with commit on job end, initiated by
     * {@link SparkExecutionContext#fromDataset(SparkContext, String, scala.collection.immutable.Map,
     * Option, ClassTag, ClassTag)}
     */
    IMPLICIT_COMMIT_ON_JOB_END
  }

  /**
   * Property key for storing the key to lookup active transaction
   */
  static final String ACTIVE_TRANSACTION_KEY = "cdap.spark.active.transaction";

  private static final Logger LOG = LoggerFactory.getLogger(SparkTransactional.class);

  private final ThreadLocal<TransactionalDatasetContext> activeDatasetContext =
    new ThreadLocal<TransactionalDatasetContext>() {
      @Override
      public void set(TransactionalDatasetContext value) {
        String txKey = Long.toString(value.getTransaction().getWritePointer());
        if (SparkRuntimeEnv.setLocalProperty(ACTIVE_TRANSACTION_KEY, txKey)) {
          transactionInfos.put(txKey, value);
        }

        super.set(value);
      }

      @Override
      public void remove() {
        String txKey = SparkRuntimeEnv.getLocalProperty(ACTIVE_TRANSACTION_KEY);
        if (txKey != null && !txKey.isEmpty()) {
          // Spark doesn't support unsetting of property. Hence set it to empty.
          SparkRuntimeEnv.setLocalProperty(ACTIVE_TRANSACTION_KEY, "");
          transactionInfos.remove(txKey);
        }
        super.remove();
      }
  };

  private final TransactionSystemClient txClient;
  private final DynamicDatasetCache datasetCache;
  private final Map<String, TransactionalDatasetContext> transactionInfos;

  SparkTransactional(TransactionSystemClient txClient, DynamicDatasetCache datasetCache) {
    this.txClient = txClient;
    this.datasetCache = datasetCache;
    this.transactionInfos = new ConcurrentHashMap<>();
  }

  /**
   * Executes the given {@link TxRunnable} with a long {@link Transaction}. All Spark RDD operations performed
   * inside the given {@link TxRunnable} will be using the same {@link Transaction}.
   *
   * @param runnable the runnable to be executed in the transaction
   * @throws TransactionFailureException if there is failure during execution. The actual cause of the failure
   *                                     maybe wrapped inside the {@link TransactionFailureException} (both
   *                                     user exception from the {@link TxRunnable#run(DatasetContext)} method
   *                                     or transaction exception from Tephra).
   */
  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    execute(wrap(runnable), TransactionType.EXPLICIT);
  }

  @Nullable
  TransactionInfo getTransactionInfo(String key) {
    return transactionInfos.get(key);
  }

  /**
   * Executes the given runnable with transactionally. If there is an opened transaction that can be used, then
   * the runnable will be executed with that existing transaction.
   * Otherwise, a new long transaction will be created to exeucte the given runnable.
   *
   * @param runnable The {@link TxRunnable} to be executed inside a transaction
   * @param transactionType The {@link TransactionType} of the Spark transaction.
   *
   */
  void execute(SparkTxRunnable runnable, TransactionType transactionType) throws TransactionFailureException {
    TransactionalDatasetContext txDatasetContext = activeDatasetContext.get();
    boolean needCommit = false;

    // If there is an existing transaction
    if (txDatasetContext != null) {
      TransactionType currentTransactionType = txDatasetContext.getTransactionType();

      // We don't support nested transaction
      if (currentTransactionType == TransactionType.EXPLICIT && transactionType == TransactionType.EXPLICIT) {
        throw new TransactionFailureException("Nested transaction not supported. Active transaction is "
                                                + txDatasetContext.getTransaction());
      }

      // If the current transaction is commit on job end, we need some special handling
      if (currentTransactionType == TransactionType.IMPLICIT_COMMIT_ON_JOB_END) {
        // If the job already started, we need to wait for the job completion so that the active transaction
        // gets committed before we start a new one. The waiting will block this thread until the job that
        // associated with the transaction is completed (asynchronously).
        if (txDatasetContext.isJobStarted()) {
          try {
            txDatasetContext.awaitCompletion();
            txDatasetContext = null;
          } catch (InterruptedException e) {
            // Don't execute the runnable. Reset the interrupt flag and return
            Thread.currentThread().interrupt();
            return;
          }
        } else if (transactionType != TransactionType.IMPLICIT_COMMIT_ON_JOB_END) {
          // If the job hasn't been started and the requested type is not commit on job end,
          // we need to "upgrade" the transaction type based on the requested type
          // E.g. if the requested type is EXPLICIT, then the current transaction will become an explicit one
          txDatasetContext.setTransactionType(transactionType);
          needCommit = true;
        }
      }
    }

    // If there is no active transaction, start a new long transaction
    if (txDatasetContext == null) {
      txDatasetContext = new TransactionalDatasetContext(txClient.startLong(), datasetCache, transactionType);
      activeDatasetContext.set(txDatasetContext);
      needCommit = transactionType != TransactionType.IMPLICIT_COMMIT_ON_JOB_END;
    }

    Transaction transaction = txDatasetContext.getTransaction();
    try {
      // Call the runnable
      runnable.run(txDatasetContext);

      // Persist the changes
      txDatasetContext.flush();

      if (needCommit) {
        // Commit transaction
        if (!txClient.commit(transaction)) {
          throw new TransactionFailureException("Failed to commit explicit transaction " + transaction);
        }
        activeDatasetContext.remove();
        txDatasetContext.postCommit();
        txDatasetContext.discardDatasets();
      }
    } catch (Throwable t) {
      // Any exception will cause invalidation of the transaction
      activeDatasetContext.remove();
      Transactions.invalidateQuietly(txClient, transaction);
      throw Transactions.asTransactionFailure(t);
    }
  }


  private SparkTxRunnable wrap(final TxRunnable runnable) {
    return new SparkTxRunnable() {
      @Override
      public void run(SparkDatasetContext context) throws Exception {
        runnable.run(context);
      }
    };
  }

  /**
   * A {@link DatasetContext} to be used for the transactional execution. All {@link Dataset} instance
   * created through instance of this class will be using the same transaction.
   *
   * Instance of this class is safe to use from multiple threads concurrently. This is for supporting Spark program
   * with multiple threads that drive computation concurrently within the same transaction.
   */
  @ThreadSafe
  private final class TransactionalDatasetContext implements SparkDatasetContext, TransactionInfo {

    private final Transaction transaction;
    private final DynamicDatasetCache datasetCache;
    private final Set<Dataset> datasets;
    private final Set<Dataset> discardDatasets;
    private TransactionType transactionType;
    private CountDownLatch completion;
    private volatile boolean jobStarted;

    private TransactionalDatasetContext(Transaction transaction,
                                        DynamicDatasetCache datasetCache, TransactionType transactionType) {
      this.transaction = transaction;
      this.datasetCache = datasetCache;
      this.datasets = Collections.synchronizedSet(new HashSet<Dataset>());
      this.discardDatasets = Collections.synchronizedSet(new HashSet<Dataset>());
      this.transactionType = transactionType;
      this.completion = new CountDownLatch(1);
    }

    boolean isJobStarted() {
      return jobStarted;
    }

    @Override
    @Nonnull
    public Transaction getTransaction() {
      return transaction;
    }

    @Override
    public boolean commitOnJobEnded() {
      return transactionType == TransactionType.IMPLICIT_COMMIT_ON_JOB_END;
    }

    @Override
    public void onJobStarted() {
      jobStarted = true;
    }

    @Override
    public void onTransactionCompleted(boolean jobSucceeded, @Nullable TransactionFailureException failureCause) {
      // Shouldn't happen
      Preconditions.checkState(commitOnJobEnded(), "Not expecting transaction to be completed");
      transactionInfos.remove(Long.toString(transaction.getWritePointer()));
      if (failureCause == null) {
        postCommit();
      }
      completion.countDown();
    }

    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      return getDataset(name, Collections.<String, String>emptyMap());
    }

    @Override
    public <T extends Dataset> T getDataset(String name,
                                            Map<String, String> arguments) throws DatasetInstantiationException {
      return getDataset(name, arguments, AccessType.UNKNOWN);
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments,
                                            AccessType accessType) throws DatasetInstantiationException {
      T dataset = datasetCache.getDataset(name, arguments, accessType);

      // Only call startTx if the dataset hasn't been seen before
      // It is ok because there is only one transaction in this DatasetContext
      // If a dataset instance is being reused, we don't need to call startTx again.
      // It's also true for the case when a dataset instance was released and reused.
      if (datasets.add(dataset) && dataset instanceof TransactionAware) {
        ((TransactionAware) dataset).startTx(transaction);
      }

      return dataset;
    }

    @Override
    public void releaseDataset(Dataset dataset) {
      discardDataset(dataset);
    }

    @Override
    public void discardDataset(Dataset dataset) {
      discardDatasets.add(dataset);
    }

    /**
     * Flushes all {@link TransactionAware} that were acquired through this {@link DatasetContext} by calling
     * {@link TransactionAware#commitTx()}.
     *
     * @throws TransactionFailureException if any {@link TransactionAware#commitTx()} call throws exception
     */
    private void flush() throws TransactionFailureException {
      for (TransactionAware txAware : Iterables.filter(datasets, TransactionAware.class)) {
        try {
          if (!txAware.commitTx()) {
            throw new TransactionFailureException("Failed to persist changes for " + txAware);
          }
        } catch (Throwable t) {
          throw Transactions.asTransactionFailure(t);
        }
      }
    }

    /**
     * Calls {@link TransactionAware#postTxCommit()} methods on all {@link TransactionAware} acquired through this
     * {@link DatasetContext}.
     */
    private void postCommit() {
      for (TransactionAware txAware : Iterables.filter(datasets, TransactionAware.class)) {
        try {
          txAware.postTxCommit();
        } catch (Exception e) {
          LOG.warn("Exception raised in postTxCommit call on dataset {}", txAware, e);
        }
      }
    }

    /**
     * Discards all datasets that has {@link #discardDataset(Dataset)} called before
     */
    private void discardDatasets() {
      for (Dataset dataset : discardDatasets) {
        datasetCache.discardDataset(dataset);
      }
      discardDatasets.clear();
      datasets.clear();
    }

    /**
     * Block until the transaction used by this context is completed (either commit or invalidate).
     *
     * @throws InterruptedException if current thread is interrupted while waiting
     */
    private void awaitCompletion() throws InterruptedException {
      LOG.debug("Awaiting completion for {}", transaction.getWritePointer());
      if (completion != null) {
        completion.await();
      }
      discardDatasets();
    }

    /**
     * Returns the {@link TransactionType}.
     */
    TransactionType getTransactionType() {
      return transactionType;
    }

    /**
     * Sets the {@link TransactionType}.
     */
    void setTransactionType(TransactionType transactionType) {
      this.transactionType = transactionType;
    }
  }
}