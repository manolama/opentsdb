package net.opentsdb.storage;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import net.opentsdb.core.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class RTRabbitMQ extends RTPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(RTRabbitMQ.class);
  
  private final ImmutableList<String> hosts;
  private String dp_exchange;
  private String vhost;
  private String user;
  private String pass;
  private int port;
  private int host_index;

  public RTRabbitMQ(final String host){
    super();
    this.vhost = "/opentsdb";
    this.dp_exchange = "datapoints";
    this.user = "guest";
    this.pass = "guest";
    this.port = 5672;
    
    String[] splits = host.split(";");
    Builder<String> host_list = ImmutableList.<String>builder();
    for (String h : splits){
      host_list.add(h);
    }
    hosts = host_list.build();
    host_index = 0;
    
    this.initializeWorkers();
  }

  protected void initializeWorkers(){
    this.datapoint_workers = new MQWorker[5];
    for (int i=0; i<this.datapoint_workers.length; i++){
      this.host_index = this.host_index >= this.hosts.size() - 1 ? 0 : this.host_index + 1;      
      MQConnection conn = new MQConnection(hosts, user, pass, 
          port, vhost, dp_exchange, true, host_index);
      this.datapoint_workers[i] = new MQWorker(this.datapoint_queue, conn);
      this.datapoint_workers[i].start();
    }
    
    LOG.debug("Initialized [" + this.datapoint_workers.length + "] datapoint threads");
  }
  
  private class MQWorker extends RTDatapointWorker {
    private final MQConnection connection;
    
    public MQWorker(final ArrayBlockingQueue<DataPoint> datapoint_queue, 
        final MQConnection connection) {
      super(datapoint_queue);
      this.connection = connection;
    }
    
    public void run(){
      JSON codec = null;
      Channel channel = null;
      DataPoint dp = null;
      while (true){
        try {
          dp = datapoint_queue.take();
          codec = new JSON(dp);
          
          channel = this.connection.getChannel();
          if (channel == null){
            // log and fail
          }else{
            channel.basicPublish(connection.exchange, dp.metric
                ,MessageProperties.BASIC, codec.getJsonBytes()) ;
          }          
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }
  
  private class MQConnection{    
    private final String user;
    private final String pass;
    private final int port;
    private final String vhost;
    private final String exchange;
    private final boolean exchange_delcare;
    private final ImmutableList<String> hosts;
    private int host_index;
    
    private Connection connection;
    private Channel channel;

    public MQConnection(final ImmutableList<String> hosts, final String user, final String pass, 
        final int port, final String vhost, final String exchange, final boolean exchange_declare,
        final int host_index){
      this.hosts = ImmutableList.copyOf(hosts);
      this.user = user;
      this.pass = pass;
      this.port = port;
      this.vhost = vhost;
      this.exchange = exchange;
      this.exchange_delcare = exchange_declare;
      this.host_index = host_index;
    }

    /**
     * Attempts to re/connect and open a channel
     * @return A channel if successful, null if we couldn't connect at all
     */
    public final Channel getChannel(){
      if (this.channel != null){
        if (this.channel.isOpen())
          return this.channel;
      }
      
      // try to reopen
      if (this.connect())
        return this.channel;
      return null;
    }
    
    public void close(){
      try {

          if (this.channel != null)
            this.channel.close();
          this.channel = null;
          
          if (this.connection != null)
            this.connection.close();
          this.connection = null;

      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    private final boolean connect(){   
      // close if open
      if (this.channel != null || this.connection != null)
        this.close();

      final ConnectionFactory factory = new ConnectionFactory();
      factory.setUsername(this.user);
      factory.setPassword(this.pass);
      factory.setHost(this.hosts.get(this.host_index));
      this.host_index = this.host_index >= this.hosts.size() - 1 ? 0 : this.host_index + 1;
      factory.setVirtualHost(this.vhost);
      factory.setPort(this.port);
      
      try {
        this.connection = factory.newConnection();
        this.channel = this.connection.createChannel();
        LOG.debug("Connected to MQ broker at [" + factory.getHost() + "]");
        return true;
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return false;
    }
    
  }
}
