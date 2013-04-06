// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.HashMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Ignore;
import org.powermock.reflect.Whitebox;

/**
 * Helper class that provides mockups for testing any OpenTSDB processes that
 * deal with Netty.
 */
@Ignore
public final class NettyMocks {

  /**
   * Sets up a TSDB object for HTTP RPC tests that has a Config object
   * @return A TSDB mock
   */
  public static TSDB getMockedHTTPTSDB() {
    final TSDB tsdb = mock(TSDB.class);
    final Config config = mock(Config.class);
    HashMap<String, String> properties = new HashMap<String, String>();
    properties.put("tsd.http.show_stack_trace", "true");
    Whitebox.setInternalState(config, "properties", properties);
    when(tsdb.getConfig()).thenReturn(config);
    return tsdb;
  }
  
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
   * Returns an HttpQuery with a mocked channel, used for URI parsing and
   * static method examples
   * @param uri a URI to use
   * @return an HttpQuery object
   */
  public HttpQuery getQuery(final TSDB tsdb, final String uri) {
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, uri);
    req.setMethod(HttpMethod.GET);
    return new HttpQuery(tsdb, req, channelMock);
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
