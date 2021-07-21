package com.google.cloud.bigquery.connector.common;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages reading ahead from an iterator and dividing it across multiple iterators that can be read
 * in a round-robin fashion.
 *
 * <p>Useful to parallelizing work from an iterator where order must still be maintained.
 *
 * @param <T> Type of iterable object.
 */
public class IteratorMultiplexer<T> implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(IteratorMultiplexer.class);
  private static final Object TERMINAL_SENTINEL = new Object();
  private final Iterator<T> iterator;
  private final int splits;
  private final QueueIterator<T>[] iterators;
  private Thread worker;

  /**
   * Construct a new instance.
   *
   * @param iterator The Iterator to read from.
   * @param splits The number of output iterators that will read from iterator.
   */
  public IteratorMultiplexer(Iterator<T> iterator, int splits) {
    this.iterator = iterator;
    this.splits = splits;

    // Filled in when initializing iterators.
    iterators = new QueueIterator[splits];
    for (int x = 0; x < splits; x++) {
      iterators[x] = new QueueIterator<>();
    }
  }

  @Override
  public void close() {
    if (worker != null) {
      worker.interrupt();
      try {
        worker.join(/*millis=*/ 1000);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting on worker thread shutdown.", e);
      }
      worker = null;
    }
    for (int x = 0; x < splits; x++) {
      iterators[x].markDone(/*exception=*/ null);
    }
  }

  void readAhead() {
    Throwable e = null;
    try {
      boolean hasMore = true;
      while (hasMore) {
        for (int x = 0; x < splits; x++) {
          if (iterator.hasNext()) {
            T value = iterator.next();
            iterators[x].sem.acquire();
            iterators[x].queue.put(value);
          } else {
            hasMore = false;
            break;
          }
        }
      }
    } catch (InterruptedException ex) {
      log.info("Worker was interrupted. Ending all iterators");
      e = new RuntimeException(ex);
    } catch (Throwable ex) {
      log.info("Worker had exception. Ending all iterators", e);
      e = ex;
    }
    for (int x = 0; x < splits; x++) {
      iterators[x].markDone(e);
    }
  }

  public synchronized Iterator<T> getSplit(int split) {
    if (worker == null) {
      worker = new Thread(this::readAhead, "readahead-worker");
      worker.setDaemon(true);
      worker.start();
    }
    return iterators[split];
  }

  private class QueueIterator<T> implements Iterator<T> {
    private final ArrayBlockingQueue<Object> queue = new ArrayBlockingQueue<>(/*capacity=*/ 2);
    private final Semaphore sem = new Semaphore(1);

    private Object t = null;

    @Override
    public boolean hasNext() {
      if (t == TERMINAL_SENTINEL) {
        return false;
      }
      try {
        t = queue.take();
        sem.release();
      } catch (InterruptedException e) {
        // We expect all iterators to either make progress together or finish.
        // This starts the cleanup process to halt all workers.
        worker.interrupt();
        t = TERMINAL_SENTINEL;
      }
      return t != TERMINAL_SENTINEL;
    }

    @Override
    public T next() {
      Preconditions.checkState(t != TERMINAL_SENTINEL, "No next message");
      if (t instanceof Throwable) {
        if (t instanceof RuntimeException) {
          throw (RuntimeException) t;
        } else {
          throw new RuntimeException((Throwable) t);
        }
      }
      T ret = (T) t;
      t = null;
      return ret;
    }

    public synchronized void markDone(Throwable e) {
      if (t == TERMINAL_SENTINEL || t instanceof Exception) {
        return;
      }
      if (queue.remainingCapacity() > 0) {
        if (e != null) {
          Preconditions.checkState(queue.offer(e), "Expected room for exception");
        } else {
          Preconditions.checkState(queue.offer(TERMINAL_SENTINEL), "Expected room for sentinel");
        }
      }
    }
  }
}
