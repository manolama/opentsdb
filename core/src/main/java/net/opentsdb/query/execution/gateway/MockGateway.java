package net.opentsdb.query.execution.gateway;

import java.security.Principal;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import net.opentsdb.auth.AuthState;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.stats.QueryStats;
import net.opentsdb.stats.Span;

public class MockGateway {

  MockQueryQueue query_queue;
  BlockingQueue<GatewayResult> result_queue;
  
  MockConsumer consumer;
  Map<String, GatewayReceiver> receivers;
  boolean diediedie;
  int per_user = 3;
  int queue_limit = 4;
  
  MockGateway() {
    result_queue = new ArrayBlockingQueue<GatewayResult>(1000);
    consumer = new MockConsumer();
    receivers = Maps.newConcurrentMap();
    query_queue = new MockQueryQueue();
    
  }
  
  class MockQueryQueue implements QueryQueue {
    Map<String, BlockingQueue<GatewayQuery>> user_queues;
    BlockingQueue<BlockingQueue<GatewayQuery>> main_queue;
    Timer timer;
    
    MockQueryQueue() {
      user_queues = Maps.newConcurrentMap();
      main_queue = new ArrayBlockingQueue<BlockingQueue<GatewayQuery>>(1000);
      timer = new HashedWheelTimer();
    }
    
    @Override
    public Object add(long hash, List<TimeSeriesQuery> queries, QueryPipelineContext ctx) {
      BlockingQueue<GatewayQuery> user_queue = user_queues.get(ctx.queryContext().authState().getUser());
      
      if (user_queue == null) {
        user_queue = new ArrayBlockingQueue<GatewayQuery>(1000);
        BlockingQueue<GatewayQuery> extant = user_queues.putIfAbsent(ctx.queryContext().authState().getUser(), user_queue);
        if (extant != null) {
          user_queue = extant;
        }
      }
      
      for (final TimeSeriesQuery query : queries) {
        MockGQuery q = new MockGQuery(hash, query, ctx);
        q.receivers = Lists.newArrayList(new MockReceiver());
        user_queue.add(q);
      }
      
      try {
        int has = 0;
        Iterator<BlockingQueue<GatewayQuery>> it = main_queue.iterator();
        while (it.hasNext()) {
          BlockingQueue<GatewayQuery> uq = it.next();
          if (uq == user_queue) {
            has++;
            if (has >= per_user) {
              System.out.println("   already queued one for this user " + has);
              return null;
            }
          }
        }
        
        if (main_queue.size() >= queue_limit && has == 0) {
          // only need to add if the queue didn't have a user entry.
          return null;
        }
        main_queue.put(user_queue);
        
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
      return null;
    }

    @Override
    public void poll(long timeout, GatewayWorker worker) {
      try {
        BlockingQueue<GatewayQuery> uq = main_queue.poll(timeout, TimeUnit.MILLISECONDS);
        if (uq == null) {
          worker.onNext(null);
        } else {
          GatewayQuery query = uq.poll();
          worker.onNext(query);
          if (query != null) {
            BlockingQueue<GatewayQuery> user_queue = user_queues.get(query.user().getUser());
            if (user_queue != null && !user_queue.isEmpty()) {
              main_queue.put(uq);
            }
          }
        }
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
  }
  
  class MockGQuery implements GatewayQuery {
    TimeSeriesQuery query;
    QueryPipelineContext ctx;
    long hash;
    List<GatewayReceiver> receivers;
    
    public MockGQuery(long hash, TimeSeriesQuery query, QueryPipelineContext ctx) {
      this.hash = hash;
      this.query = query;
      this.ctx = ctx;
    }
    
    public AuthState user() {
      return ctx.queryContext().authState();
    }
    
    @Override
    public long hash() {
      return hash;
    }

    @Override
    public TimeSeriesQuery query() {
      return query;
    }

    @Override
    public List<GatewayReceiver> receivers() {
      return receivers;
    }
    
  }
  
  class MockConsumer implements GatewayConsumer {
    
    Map<Long, QueryPipelineContext> pipelines = Maps.newConcurrentMap();
    
    @Override
    public void register(long hash, QueryPipelineContext context) {
      if (pipelines.putIfAbsent(hash, context) != null) {
        throw new RuntimeException("WTF? Already exists: " + hash);
      }
    }
    
    public void run() {
      while (true) {
        if (diediedie) {
          return;
        }
        try {
          final GatewayResult result = result_queue.take();
          QueryPipelineContext pipeline = pipelines.get(result.hash());
          if (pipeline == null) {
            System.out.println("No pipeline for result hash: " + result.hash());
            continue;
          }
          
          if (result.exception() != null) {
            pipeline.onError(result.exception());
          } else {
            pipeline.onNext(result.result());
          }
          
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
      }
    }
  }
  
  class MockReceiver implements GatewayReceiver {

    @Override
    public void send(final GatewayResult result) {
      result_queue.offer(result);
    }
    
  }

  class MockWorker extends Thread implements GatewayWorker {
    
    @Override
    public void run() {
      while(true) {
        if (diediedie) {
          return;
        }
        try {
          query_queue.poll(1000, this);
          Thread.sleep(100);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
      }
    }

    @Override
    public void onError(Throwable t) {
      // TODO Auto-generated method stub
      System.out.println("GOT AN ERROR: " + t.getMessage());
    }

    @Override
    public void onNext(GatewayQuery query) {
      if (query == null) {
        return;
      }
      
      System.out.println("GOT A QUERY: " + query.hash() + ":" +
        ((net.opentsdb.query.pojo.TimeSeriesQuery) query.query()).getOrder()
        + " [" + query.user().getUser() + "]");
      
      for (GatewayReceiver receiver : query.receivers()) {
        receiver.send(new GatewayResult() {

          @Override
          public long hash() {
            return query.hash();
          }

          @Override
          public QueryResult result() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public Throwable exception() {
            return new RuntimeException("WOOT!: " + query);
          }

          @Override
          public TimeSeriesQuery query() {
            return query.query();
          }
          
        });
      }
    }

    @Override
    public void onComplete() {
      // TODO Auto-generated method stub
      
    }
  }
  
  class MockPipeline implements QueryPipelineContext {

    final String user;
    
    MockPipeline(String user) {
      this.user = user;
    }
    
    @Override
    public QueryPipelineContext pipelineContext() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public QueryNodeConfig config() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public String id() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void onComplete(QueryNode downstream, long final_sequence,
        long total_sequences) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onNext(QueryResult next) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void onError(Throwable t) {
      // TODO Auto-generated method stub
      System.out.println("RESULT: " + t.getMessage());
    }

    @Override
    public TimeSeriesQuery query() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public QueryContext queryContext() {
      return new QueryContext() {

        @Override
        public Collection<QuerySink> sinks() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public QueryMode mode() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public void fetchNext(Span span) {
          // TODO Auto-generated method stub
          
        }

        @Override
        public void close() {
          // TODO Auto-generated method stub
          
        }

        @Override
        public AuthState authState() {
          // TODO Auto-generated method stub
          return new AuthState() {

            @Override
            public String getUser() {
              return user;
            }

            @Override
            public Principal getPrincipal() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public AuthStatus getStatus() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public String getMessage() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public Throwable getException() {
              // TODO Auto-generated method stub
              return null;
            }

            @Override
            public byte[] getToken() {
              // TODO Auto-generated method stub
              return null;
            }
            
          };
        }

        @Override
        public QueryStats stats() {
          // TODO Auto-generated method stub
          return null;
        }

        @Override
        public TimeSeriesQuery query() {
          // TODO Auto-generated method stub
          return null;
        }
        
      };
    }

    @Override
    public void initialize() {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void fetchNext(Span span) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public Collection<QueryNode> upstream(QueryNode node) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<QueryNode> downstream(QueryNode node) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<TimeSeriesDataSource> downstreamSources(QueryNode node) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<QuerySink> sinks() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<QueryNode> roots() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub
      
    }
    
  }

  static Random rnd = new Random(System.currentTimeMillis());
  
  static class QGen extends Thread {
    final MockGateway gateway;
    final String user;
    final AtomicInteger cnt;
    final int idx;
    
    QGen(MockGateway gateway, String user, AtomicInteger cnt, int idx) {
      this.gateway = gateway;
      this.user = user;
      this.cnt = cnt;
      this.idx = idx;
    }
    
    @Override
    public void run() {
      List<TimeSeriesQuery> queries = Lists.newArrayList();
      for (int i = 0; i < rnd.nextInt(10); i++) {
        TimeSeriesQuery q = net.opentsdb.query.pojo.TimeSeriesQuery.newBuilder()
            .setTime(Timespan.newBuilder()
                .setStart("1h-ago"))
            .addMetric(Metric.newBuilder()
                .setMetric("m" + i))
            .setOrder(i)
            .build();
        queries.add(q);
      }
      cnt.addAndGet(queries.size());
      gateway.query_queue.add(idx, queries, gateway.new MockPipeline(user));
    }
  }
  
  public static void main(final String[] args) throws Exception {
    
    MockGateway gateway = new MockGateway();
    List<MockWorker> workers = Lists.newArrayList();
    for (int i = 0; i < 2; i++) {
      MockWorker worker = gateway.new MockWorker();
      worker.start();
      workers.add(worker);
    }
    AtomicInteger cnt = new AtomicInteger();
    List<String> users = Lists.newArrayList("clarsen", "bob", "Tyrion", "Florence");
    for (int i = 0; i < 25; i++) {
      int s = rnd.nextInt(users.size());
      new QGen(gateway, users.get(s > 0 ? s - 1 : 0), cnt, i).start();
      Thread.sleep(10);
    }
    System.out.println("WROTE: " + cnt.get());
    try {
      Thread.sleep(200);
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    gateway.diediedie = true;
    gateway.query_queue.timer.stop();
    Thread.sleep(1000);
    System.out.println("DONE");
  }
}
