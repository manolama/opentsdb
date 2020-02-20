package net.opentsdb.utils;

import java.util.List;
import java.util.function.Predicate;

import com.google.common.collect.Lists;

public class MockBigSmallLinkedBlockingQueue<T> implements TSDBQueryQueue<T> {

  public final boolean run_immediately;
  public final Predicate<T> big_predicate;
  public final List<T> big_queue;
  public final List<T> small_queue;
  
  public MockBigSmallLinkedBlockingQueue(final boolean run_immediately,
                                         final Predicate<T> big_predicate) {
    this.run_immediately = run_immediately;
    this.big_predicate = big_predicate;
    big_queue = Lists.newArrayList();
    small_queue = Lists.newArrayList();
  }
  
  @Override
  public void put(final T t) {
    boolean is_big = big_predicate.test(t);
    if (is_big) {
      big_queue.add(t);
    } else {
      small_queue.add(t);
    }
    
    if (t instanceof Runnable) {
      ((Runnable) t).run();
    }
  }

  @Override
  public T take() throws InterruptedException {
    return null;
  }

  @Override
  public int size() {
    return big_queue.size() + small_queue.size();
  }

  @Override
  public void shutdown() {
  }

  public void reset() {
    big_queue.clear();
    small_queue.clear();
  }
  
  public void runAll() {
    for (int i = 0; i < big_queue.size(); i++) {
      if (big_queue.get(i) instanceof Runnable) {
        ((Runnable) big_queue.get(i)).run();
      }
    }
    
    for (int i = 0; i < small_queue.size(); i++) {
      if (small_queue.get(i) instanceof Runnable) {
        ((Runnable) small_queue.get(i)).run();
      }
    }
  }
}
