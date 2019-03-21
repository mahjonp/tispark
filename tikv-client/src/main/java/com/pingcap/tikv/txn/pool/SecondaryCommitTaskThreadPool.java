/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.txn.pool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 2PC: Secondary commit thread pool */
public final class SecondaryCommitTaskThreadPool implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SecondaryCommitTaskThreadPool.class);

  private ExecutorService taskThreadsPool;

  public SecondaryCommitTaskThreadPool() {
    this.taskThreadsPool = Executors.newWorkStealingPool();
  }

  public String submitSecondaryTask(Runnable task) {
    try {
      this.taskThreadsPool.submit(task);
      return null;
    } catch (Exception e) {
      LOG.error("Submit secondary task failed");
      return String.format("Submit secondary task failed");
    }
  }

  @Override
  public void close() throws Exception {
    if (taskThreadsPool != null) {
      if (!taskThreadsPool.awaitTermination(20, TimeUnit.SECONDS)) {
        taskThreadsPool.shutdownNow(); // Cancel currently executing tasks
      } else {

      }
    }
  }
}
