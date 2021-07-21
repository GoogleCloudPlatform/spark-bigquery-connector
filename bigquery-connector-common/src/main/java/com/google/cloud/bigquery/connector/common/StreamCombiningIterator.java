package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.rpc.ResponseObserver;
import com.google.api.gax.rpc.StreamController;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * An iterator that combines one or more ReadRows requests into a single iterator. Ordering of
 * messages is not guaranteed from the streams.
 *
 * <p>This is adapted from ServerStream in gax. In comparison toServerStream, this implementation
 * also buffers more a configured number of responses instead of a single one. For connections with
 * high latency between client and server this can processing costs. It also allows combining one or
 * more ReadRows calls into single iterator to potentially increase perceived client throughput if
 * that becomes a bottleneck for processing.
 */
public class StreamCombiningIterator implements Iterator<ReadRowsResponse> {
  private static final Object EOS = new Object();
  // Contains either a ReadRowsResponse, or a terminal object of throwable OR EOS
  // This class is engineered keep responses below capacity unless a terminal state has
  // been reached.
  private final ArrayBlockingQueue<Object> responses;
  private final ArrayBlockingQueue<Observer> observersQueue;
  private final AtomicInteger observersLeft;
  private final int bufferEntriesPerStream;
  private final int numRetries;
  private final Object lock = new Object();
  private final BigQueryReadClient client;
  Object last;
  volatile boolean cancelled = false;
  private final Collection<Observer> observers;

  StreamCombiningIterator(
      BigQueryReadClient client,
      Collection<ReadRowsRequest.Builder> requests,
      int bufferEntriesPerStream,
      int numRetries) {
    this.client = client;
    observersLeft = new AtomicInteger(requests.size());
    this.bufferEntriesPerStream = bufferEntriesPerStream;
    Preconditions.checkArgument(
        this.bufferEntriesPerStream > 0,
        "bufferEntriesPerstream must be positive.  Received: %s",
        this.bufferEntriesPerStream);
    // + 1 to leave space for terminal object.
    responses = new ArrayBlockingQueue<>((requests.size() * this.bufferEntriesPerStream) + 1);
    observersQueue = new ArrayBlockingQueue<>(requests.size() * this.bufferEntriesPerStream);
    this.numRetries = numRetries;
    observers = requests.stream().map(Observer::new).collect(Collectors.toList());
  }

  synchronized void stopWithError(Throwable error) {
    synchronized (lock) {
      if (cancelled) {
        return;
      }

      try {
        completeStream(/*addEos=*/ false);
      } finally {
        Preconditions.checkState(
            responses.add(error), "Responses should always have capacity to add element");
      }
    }
  }

  private boolean hasActiveObservers() {
    return observersLeft.get() > 0;
  }

  /**
   * Consumes the next response and asynchronously request the next response from the observer.
   *
   * @return The next response.
   * @throws NoSuchElementException If the stream has been consumed or cancelled.
   */
  @Override
  public ReadRowsResponse next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    try {
      Observer observer = observersQueue.poll();
      Preconditions.checkState(observer != null);
      observer.request();
      @SuppressWarnings("unchecked")
      ReadRowsResponse tmp = (ReadRowsResponse) last;
      return tmp;
    } finally {
      if (last != EOS) {
        last = null;
      }
    }
  }

  /**
   * Checks if the stream has been fully consumed or cancelled. This method will block until the
   * observer enqueues another event (response or completion event). If the observer encountered an
   * error, this method will propagate the error and put itself into an error where it will always
   * return the same error.
   *
   * @return true If iterator has more responses.
   */
  @Override
  public boolean hasNext() {
    if (last == null) {
      try {
        last = responses.take();
      } catch (InterruptedException e) {
        cancel();
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    // Preserve async error while keeping the caller's stacktrace as a suppressed exception
    if (last instanceof RuntimeException) {
      RuntimeException runtimeException = (RuntimeException) last;
      runtimeException.addSuppressed(new RuntimeException("Asynchronous task failed"));
      throw runtimeException;
    }

    // This should never really happen because currently gax doesn't throw checked exceptions.
    // Wrap checked exceptions. This will preserve both the caller's stacktrace and the async error.
    if (last instanceof Throwable) {
      Throwable throwable = (Throwable) last;
      throw new UncheckedExecutionException(throwable);
    }

    return last != EOS;
  }

  public void cancel() {
    synchronized (lock) {
      if (cancelled) {
        return;
      }
      completeStream(/*addEos=*/ true);
    }
  }

  private void maybeFinished() {
    synchronized (lock) {
      if (cancelled) {
        return;
      }
      if (hasActiveObservers()) {
        return;
      }
      completeStream(/*addEos=*/ true);
    }
    for (Observer observer : observers) {
      observer.cancel();
    }
  }

  private void completeStream(boolean addEos) {
    cancelled = true;
    observersLeft.set(0);
    try {
      for (Observer observer : observers) {
        observer.cancel();
      }
    } finally {
      if (addEos) {
        responses.add(EOS);
      }
    }
  }

  private void newConnection(Observer observer, ReadRowsRequest.Builder request) {
    synchronized (lock) {
      if (!cancelled) {
        client.readRowsCallable().call(request.build(), observer);
      }
    }
  }

  class Observer implements ResponseObserver<ReadRowsResponse> {
    /* Offset into the stream that this is processing. */
    private long readRowsCount = 0;
    /* Number of retries so far on this observer */
    private int retries = 0;
    /**
     * All methods accessing controller must be synchronized using controllerLock. The states of
     * this object are: - Fresh object: null - Stream ready (receiving responses): not null -
     * Retrying stream: null - Stream Finished: null
     */
    StreamController controller;
    // Number of responses enqueued in the main iterator.
    AtomicInteger enqueuedCount = new AtomicInteger(0);
    private final Object controllerLock = new Object();

    // The ReadRows request.  Uses a builder so offset can easily be set for retry.
    ReadRowsRequest.Builder builder;

    Observer(ReadRowsRequest.Builder builder) {
      this.builder = builder;
      newConnection(this, builder);
    }

    @Override
    public void onResponse(ReadRowsResponse value) {
      readRowsCount += value.getRowCount();
      // Note we don't take a global lock here, so ordering of observers might be different then
      // responses.  This should be OK because it should balance out in the end (there should
      // never be more then bufferResponses enquered from any given observer at any time).
      // Ordering is important to ensure there is always an observer present for the given response.
      Preconditions.checkState(observersQueue.add(this));
      Preconditions.checkState(responses.add(value), "Expected capacity in responses");
      int enqueued = enqueuedCount.incrementAndGet();
    }

    @Override
    public void onStart(StreamController controller) {
      synchronized (StreamCombiningIterator.this.lock) {
        if (cancelled) {
          controller.cancel();
          return;
        }
        synchronized (controllerLock) {
          this.controller = controller;

          controller.disableAutoInboundFlowControl();
          int requestCount = bufferEntriesPerStream - enqueuedCount.get();
          if (requestCount > 0) {
            controller.request(requestCount);
          }
        }
      }
    }

    @Override
    public void onError(Throwable t) {
      // if relevant, retry the read, from the last read position
      if (BigQueryUtil.isRetryable(t) && retries < numRetries) {
        synchronized (controllerLock) {
          controller = null;
        }
        builder.setOffset(readRowsCount);
        newConnection(this, builder);
        retries++;
      } else {
        stopWithError(t);
      }
    }

    @Override
    public void onComplete() {
      synchronized (controllerLock) {
        controller = null;
      }
      int left = observersLeft.decrementAndGet();
      maybeFinished();
    }

    public synchronized void request() {
      if (cancelled) {
        return;
      }
      boolean canExit = false;
      while (!canExit) {
        synchronized (controllerLock) {
          enqueuedCount.decrementAndGet();
          if (controller == null) {
            return;
          }

          // When not null there are a few cases to consider:
          // 1.  Stream is active, so we want to request here.
          // 2.  Stream is active but onError has been called and blocked on the lock.
          //     In this case by decrementing above the new request should be taken care
          //     after clearing and creating the new stream.
          // 3.  The stream has no more messages.
          // 4.  Stream is inactive (still initializing).
          try {
            controller.request(1);
            canExit = true;
          } catch (RuntimeException e) {
            // controller might not be started and javadoc isn't clear if it is on its path
            // to shutdown on what should happen.
          }
        }
      }
    }

    public void cancel() {
      synchronized (controllerLock) {
        if (controller != null) {
          try {
            controller.cancel();
          } catch (RuntimeException e) {
            // There could be edge cases here where controller is already cancelled or not yet
            // read or something else is happening.  We don't want this to be fatal.
          }
        }
      }
    }
  }
}
