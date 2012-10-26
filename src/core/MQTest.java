package net.opentsdb.core;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class MQTest {
  protected static final Logger LOG = LoggerFactory.getLogger(MQTest.class);
  private final ConnectionFactory factory = new ConnectionFactory();
  private Connection conn;
  private Channel channel;
  private String exchange;
  
  public MQTest(final String host){
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setVirtualHost("/");
    factory.setHost(host);
    factory.setPort(5672);
    exchange = "tsdb";
  }
  
  public boolean publish(final String key, final byte[] message){
    try {
      if (channel == null){
        if (!this.connect()){
          LOG.error("Unable to connect to broker");
          return false;
        }
      }
      
      channel.basicPublish(exchange, key
          ,MessageProperties.PERSISTENT_TEXT_PLAIN, message) ;
      return true;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }
  
  private boolean connect(){
    try {
      conn = factory.newConnection();
      channel = conn.createChannel();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    conn = null;
    return false;    
  }
  
  private boolean close(){
    try {
      if (channel != null)
        channel.close();
      
      if (conn != null)
        conn.close();
      
      channel = null;
      conn = null;
      return true;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    channel = null;
    conn = null;
    return false;
  }
}
