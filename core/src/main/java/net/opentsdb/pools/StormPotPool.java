package net.opentsdb.pools;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.ObjectPoolException;
import stormpot.BlazePool;
import stormpot.PoolException;
import stormpot.Slot;

public class StormPotPool implements ObjectPool {

  private Allocator allocator;
  
  private stormpot.BlazePool<MyPoolable> stormpot;
  
  private stormpot.Timeout default_timeout;
  
  @Override
  public Poolable claim() {
    try {
      final Poolable poolable = stormpot.claim(default_timeout);
      if (poolable != null) {
        return poolable;
      }
      return new MyPoolable(allocator.allocate(), null);
    } catch (PoolException e) {
      throw new ObjectPoolException(e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Poolable claim(long time, ChronoUnit unit) {
    try {
      // arg, need jdk9+
      TimeUnit tu = null;
      switch (unit) {
      case NANOS:
        tu = TimeUnit.NANOSECONDS;
        break;
      case MICROS:
        tu = TimeUnit.MICROSECONDS;
        break;
      case MILLIS:
        tu = TimeUnit.MILLISECONDS;
        break;
      case SECONDS:
        tu = TimeUnit.SECONDS;
        break;
      default:
        throw new IllegalArgumentException("Must choose something <= seconds. "
            + "Otherwise why are you waiting so long?");
      }
      final Poolable poolable = stormpot.claim(new stormpot.Timeout(time, tu));
      if (poolable != null) {
        return poolable;
      }
      return new MyPoolable(allocator.allocate(), null);
    } catch (PoolException e) {
      throw new ObjectPoolException(e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String type() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String id() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb, String id) {
    stormpot.Config<MyPoolable> config = 
        new stormpot.Config<MyPoolable>()
        .setAllocator(new MyAllocator())
        .setSize(4096);
    stormpot = new BlazePool<MyPoolable>(config);
    default_timeout = new stormpot.Timeout(1, TimeUnit.NANOSECONDS);
    return null;
  }

  @Override
  public Deferred<Object> shutdown() {
    stormpot.shutdown();
    return null;
  }

  @Override
  public String version() {
    // TODO Auto-generated method stub
    return null;
  }
  
  class MyPoolable implements stormpot.Poolable, Poolable {
    final Object object;
    final Slot slot;
    
    MyPoolable(final Object object, final Slot slot) {
      this.object = object;
      this.slot = slot;
    }
    
    @Override
    public Object object() {
      return object;
    }
    
    @Override
    public void release() {
      if (slot != null) {
        slot.release(this);
      }
    }
    
  }

  class MyAllocator implements stormpot.Allocator<MyPoolable> {

    @Override
    public MyPoolable allocate(final Slot slot) throws Exception {
      return new MyPoolable(allocator.allocate(), slot);
    }

    @Override
    public void deallocate(MyPoolable poolable) throws Exception {
      poolable.release();
      allocator.deallocate(poolable.object);
    }
    
  }
  
}
