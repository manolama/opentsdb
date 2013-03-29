package net.opentsdb.tsd;

import net.opentsdb.core.TSDB;

import com.stumbleupon.async.Deferred;
import org.junit.Ignore;

/**
 * This is a dummy HTTP plugin Formatter implementation for unit test purposes
 * @since 2.0
 */
@Ignore
public class DummyHttpFormatter extends HttpFormatter {

  public DummyHttpFormatter() {
    super();
    this.request_content_type = "application/tsdbdummy";
    this.response_content_type = "application/tsdbdummy; charset=UTF-8";
  }
  
  public DummyHttpFormatter(final HttpQuery query) {
    super(query);
    this.request_content_type = "application/tsdbdummy";
    this.response_content_type = "application/tsdbdummy; charset=UTF-8";
  }

  @Override
  public void initialize(final TSDB tsdb) {
    // nothing to do
  }
  
  @Override
  public Deferred<Object> Shutdown() {
    return new Deferred<Object>();
  }

  @Override
  public String version() {
    return "1.0.0";
  }

  @Override
  public String shortName() {
    return "dummy";
  }

}
