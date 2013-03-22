// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO - Does this make sense? I really just wanted an easy way to track puts
 * or failures and write warnings to the log when a put fails. Also I don't know
 * if the LinkedBlockingQueue is the most performant, etc
 * 
 * A thread-safe wrapper around a blocking queue that provides a simple means
 * of tracking when writes fail and dumps alerts in the log. Only the 
 * {@link #offer()} method is exposed for queuing objects since it's meant to
 * be used by the real time plugins where we want to make a "best effort" at 
 * publishing data but don't want to drag down the main TSD process. 
 * 
 * If a put fails, it will log a warning once to avoid flooding the logs with
 * failure messages. When a subsequent put is successful, it will log another
 * message.
 * 
 * @param <T> The type of data to store in this queue
 * @since 2.0
 */
public class BoundQueue<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BoundQueue.class);
  
  /** The underlying queue */
  private final LinkedBlockingQueue<T> queue;
  
  /** A flag to let us know if the puts are in a failure state */
  private boolean put_fail;
  
  /** Counts the number of put failures due to a full queue */
  private final AtomicLong queue_full = new AtomicLong();
  
  /** Counts the number of successful puts */
  private final AtomicLong queue_puts = new AtomicLong();
  
  /**
   * Constructor that sets the upper bound of the queue
   * @param t Type of data to store
   * @param upper_bound The upper bound, must be a positive integer greater 
   * than 0
   * @throws IllegalArgumentException if the upper_bound is less than 1
   */
  public BoundQueue(final T t, final int upper_bound) {
    if (upper_bound < 1) {
      throw new IllegalArgumentException("Upper bound must be greater than 0");
    }
    this.queue = new LinkedBlockingQueue<T>(upper_bound);
  }
  
  /**
   * Attempts to store the object in the queue. If the queue is full (the 
   * upper bound has been reached) then the object will be rejected immediately
   * @param t The object to store
   * @return True if the put was successful, false if the queue was full
   */
  public boolean offer(final T t) {
    if (this.queue.offer(t)) {
      this.queue_puts.incrementAndGet();
      if (this.put_fail) {
        this.put_fail = false;
        LOG.info("Queue recovered, storing data");
      }
      return true;
    }
    
    if (!this.put_fail) {
      this.put_fail = true;
      LOG.warn("Queue was full, dropping data");
    }
    this.queue_full.incrementAndGet();
    return false;
  }
  
  /** Returns the total number of put failures */
  public long getFailureCount() {
    return this.queue_full.get();
  }
  
  /** Returns the total number of successful puts */
  public long getSuccessCount() {
    return this.queue_puts.get();
  }
  
  /**
   * Returns an object from the queue when one is available. This is a blocking
   * method that will wait until something is returnable.
   * @return An object from the top of the queue
   * @throws InterruptedException if the thread was interrupted
   */
  public T take() throws InterruptedException { 
    return this.queue.take();
  }
}
