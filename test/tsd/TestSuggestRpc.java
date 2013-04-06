// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.TSDB;

import static org.mockito.Mockito.when;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class})
public final class TestSuggestRpc {
  private TSDB tsdb = null;
  
  @Before
  public void before() {
    tsdb = NettyMocks.getMockedHTTPTSDB();
    final List<String> metrics = new ArrayList<String>();
    metrics.add("sys.cpu.0.system"); 
    when(tsdb.suggestMetrics("s")).thenReturn(metrics);
    final List<String> tagks = new ArrayList<String>();
    tagks.add("host");
    when(tsdb.suggestTagNames("h")).thenReturn(tagks);
    final List<String> tagvs = new ArrayList<String>();
    tagvs.add("web01.mysite.com");
    when(tsdb.suggestTagValues("w")).thenReturn(tagks);
  }
  
  @Test (expected = BadRequestException.class)
  public void badMethod() throws Exception {
    SuggestRpc s = new SuggestRpc();
    final Channel channelMock = NettyMocks.fakeChannel();
    final HttpRequest req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, 
        HttpMethod.GET, "api/suggest?type=metrics&q=h");
    req.setMethod(HttpMethod.PUT);
    s.execute(tsdb, new HttpQuery(tsdb, req, channelMock));
  }
}
