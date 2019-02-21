package net.opentsdb.pools;

import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import org.openjdk.jol.info.ClassLayout;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.pools.ObjectPoolException;
import net.opentsdb.pools.StormPotPool.MyAllocator;
import net.opentsdb.pools.StormPotPool.MyPoolable;
import stormpot.BlazePool;
import stormpot.PoolException;
import stormpot.Slot;

public class StormPotPool implements ObjectPool {
  private static final int SIZE = (int) (
      ClassLayout.parseClass(MyPoolable.class).instanceSize() + 8 /* ref in pool */);
  protected Allocator allocator;
  
  protected stormpot.BlazePool<MyPoolable> stormpot;
  
  protected stormpot.Timeout default_timeout;
  
  protected ObjectPoolConfig config;
  
  protected StormPotPool(final TSDB tsdb, final ObjectPoolConfig config) {
    this.config = config;
    allocator = config.getAllocator();
    stormpot.Config<MyPoolable> storm_pot_config = 
        new stormpot.Config<MyPoolable>()
        .setAllocator(new MyAllocator())
        .setSize(config.initialCount());
    stormpot = new BlazePool<MyPoolable>(storm_pot_config);
    default_timeout = new stormpot.Timeout(1, TimeUnit.NANOSECONDS);
  }
  
  @Override
  public Poolable claim() {
    try {
      final Poolable poolable = stormpot.claim(default_timeout);
      if (poolable != null) {
        return poolable;
      }
      System.out.println("$$$$$$$$$ WARN: Allocating a new one ");
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
  public int overhead() {
    return SIZE;
  }
  
  @Override
  public Deferred<Object> shutdown() {
    // TODO - WTF? Why doesn't his completion return a Void or null at least?
    stormpot.shutdown();
    return Deferred.fromResult(null);
  }
  
  @Override
  public String id() {
    return config.id();
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
