package net.opentsdb.tsd;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

/**
 * Helper class that provides mockups for testing any OpenTSDB processes that
 * deal with Netty.
 */
public class NettyMocks {

  /**
   * Returns a mocked Channel object that simply sets the name to
   * [fake channel]
   * @return A Channel mock
   */
  public static Channel fakeChannel() {
    final Channel chan = mock(Channel.class);
    when(chan.toString()).thenReturn("[fake channel]");
    return chan;
  }
  
  /**
   * Returns a simple pipeline with an HttpRequestDecoder and an 
   * HttpResponseEncoder. No mocking, returns an actual pipeline
   * @return The pipeline
   */
  private DefaultChannelPipeline createHttpPipeline() {
    DefaultChannelPipeline pipeline = new DefaultChannelPipeline();
    pipeline.addLast("requestDecoder", new HttpRequestDecoder());
    pipeline.addLast("responseEncoder", new HttpResponseEncoder());
    return pipeline;
  }
}
