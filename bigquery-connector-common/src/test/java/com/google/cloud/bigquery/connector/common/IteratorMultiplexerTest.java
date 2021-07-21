package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class IteratorMultiplexerTest {

  @Test
  public void testIteratorRoundRobins() throws InterruptedException {
    ImmutableList<Integer> values = ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    try (IteratorMultiplexer multiplexer =
        new IteratorMultiplexer<>(values.iterator(), /*multiplexedIterators=*/ 3)) {
      ImmutableList<Iterator<Integer>> iterators =
          ImmutableList.of(
              multiplexer.getSplit(0), multiplexer.getSplit(1), multiplexer.getSplit(2));
      ImmutableList<List<Integer>> multiPlexed =
          ImmutableList.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
      ExecutorService executorService = Executors.newFixedThreadPool(3);
      for (int x = 0; x < 3; x++) {
        final int idx = x;
        executorService.submit(
            () -> {
              Iterator<Integer> iterator = iterators.get(idx);
              while (iterator.hasNext()) {
                multiPlexed.get(idx).add(iterator.next());
              }
            });
      }
      executorService.shutdown();
      assertThat(executorService.awaitTermination(200, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(multiPlexed.get(0)).containsExactly(0, 3, 6, 9).inOrder();
      assertThat(multiPlexed.get(1)).containsExactly(1, 4, 7).inOrder();
      assertThat(multiPlexed.get(2)).containsExactly(2, 5, 8).inOrder();
    }
  }

  @Test
  public void testIteratorRoundRobinsOneValue() throws InterruptedException {
    ImmutableList<Integer> values = ImmutableList.of(0);

    try (IteratorMultiplexer multiplexer =
        new IteratorMultiplexer<>(values.iterator(), /*multiplexedIterators=*/ 3)) {
      ImmutableList<Iterator<Integer>> iterators =
          ImmutableList.of(
              multiplexer.getSplit(0), multiplexer.getSplit(1), multiplexer.getSplit(2));
      ImmutableList<List<Integer>> multiPlexed =
          ImmutableList.of(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
      ExecutorService executorService = Executors.newFixedThreadPool(3);
      for (int x = 0; x < 3; x++) {
        final int idx = x;
        executorService.submit(
            () -> {
              Iterator<Integer> iterator = iterators.get(idx);
              while (iterator.hasNext()) {
                multiPlexed.get(idx).add(iterator.next());
              }
            });
      }
      executorService.shutdown();
      assertThat(executorService.awaitTermination(200, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(multiPlexed.get(0)).containsExactly(0).inOrder();
      assertThat(multiPlexed.get(1)).containsExactly().inOrder();
      assertThat(multiPlexed.get(2)).containsExactly().inOrder();
    }
  }

  @Test
  public void testIteratorClosedGracefullyWhenSubIteratorsAreInterrupted()
      throws InterruptedException {
    Iterator<Integer> infiniteIterator =
        new Iterator<Integer>() {
          @Override
          public boolean hasNext() {
            return true;
          }

          @Override
          public Integer next() {
            return 0;
          }
        };
    try (IteratorMultiplexer multiplexer =
        new IteratorMultiplexer(infiniteIterator, /*multiplexedIterators=*/ 3)) {
      ImmutableList<Iterator<Integer>> iterators =
          ImmutableList.of(
              multiplexer.getSplit(0), multiplexer.getSplit(1), multiplexer.getSplit(2));
      ExecutorService executorService = Executors.newFixedThreadPool(3);
      for (int x = 0; x < 3; x++) {
        final int idx = x;
        executorService.submit(
            () -> {
              Iterator<Integer> iterator = iterators.get(idx);
              while (iterator.hasNext()) {
                iterator.next();
              }
            });
      }
      executorService.shutdownNow();
      assertThat(executorService.awaitTermination(100, TimeUnit.MILLISECONDS)).isTrue();
      assertThat(executorService.isTerminated());
      multiplexer.close();
    }
  }

  @Test
  public void testIteratorClosedGracefullyWhenMultiplexerClosed() throws InterruptedException {
    Iterator<Integer> infiniteIterator =
        new Iterator<Integer>() {
          @Override
          public boolean hasNext() {
            try {
              TimeUnit.MINUTES.sleep(1);
              return true;
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public Integer next() {
            return 0;
          }
        };
    CountDownLatch exited = new CountDownLatch(3);
    try (IteratorMultiplexer multiplexer =
        new IteratorMultiplexer(infiniteIterator, /*multiplexedIterators=*/ 3)) {
      ImmutableList<Iterator<Integer>> iterators =
          ImmutableList.of(
              multiplexer.getSplit(0), multiplexer.getSplit(1), multiplexer.getSplit(2));
      ExecutorService executorService = Executors.newFixedThreadPool(3);
      for (int x = 0; x < 3; x++) {
        final int idx = x;
        executorService.submit(
            () -> {
              Iterator<Integer> iterator = iterators.get(idx);
              try {
                while (iterator.hasNext()) {
                  iterator.next();
                }
              } finally {
                exited.countDown();
              }
            });
      }
      multiplexer.close();

      assertThat(exited.await(3, TimeUnit.SECONDS)).isTrue();
      executorService.shutdown();
    }
  }
}
