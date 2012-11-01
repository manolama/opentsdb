package net.opentsdb.formatters;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.TSDB;
import net.opentsdb.tsd.DataQuery;
import net.opentsdb.tsd.HttpQuery;

public class HTML extends TSDFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(HTML.class);
  
  public HTML(TSDB tsdb) {
    super(tsdb);
  }

  @Override
  public String getEndpoint() {
    return "html";
  }

  public String contentType(){
    return "text/html";
  }
  
  @Override
  public boolean validateQuery(DataQuery query) {
    return true;
  }

  public boolean handleHTTPRoot(final HttpQuery query){
    LOG.warn("Method has not been implemented");
    query.sendError(HttpResponseStatus.NOT_IMPLEMENTED, "Method has not been implemented");
    return false;
  }
  

  /**
   * Easy way to generate a small, simple HTML page.
   * <p>
   * Equivalent to {@code makePage(null, title, subtitle, body)}.
   * @param title What should be in the {@code title} tag of the page.
   * @param subtitle Small sentence to use next to the TSD logo.
   * @param body The body of the page (excluding the {@code body} tag).
   * @return A full HTML page.
   */
  public static StringBuilder makePage(final String title,
      final String subtitle, final String body) {
    return makePage(null, title, subtitle, body);
  }

  /**
   * Easy way to generate a small, simple HTML page.
   * @param htmlheader Text to insert in the {@code head} tag. Ignored if
   *          {@code null}.
   * @param title What should be in the {@code title} tag of the page.
   * @param subtitle Small sentence to use next to the TSD logo.
   * @param body The body of the page (excluding the {@code body} tag).
   * @return A full HTML page.
   */
  public static StringBuilder makePage(final String htmlheader,
      final String title, final String subtitle, final String body) {
    final StringBuilder buf = new StringBuilder(BOILERPLATE_LENGTH
        + (htmlheader == null ? 0 : htmlheader.length()) + title.length()
        + subtitle.length() + body.length());
    buf.append(PAGE_HEADER_START).append(title).append(PAGE_HEADER_MID);
    if (htmlheader != null) {
      buf.append(htmlheader);
    }
    buf.append(PAGE_HEADER_END_BODY_START).append(subtitle)
        .append(PAGE_BODY_MID).append(body).append(PAGE_FOOTER);
    return buf;
  }


  // -------------------------------------------- //
  // Boilerplate (shamelessly stolen from Google) //
  // -------------------------------------------- //

  private static final String PAGE_HEADER_START = "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">\n"
      + "<html>\n<head>\n"
      + "\t<meta http-equiv=content-type content=\"text/html;charset=utf-8\">\n"
      + "\t<title>\n\n";

  private static final String PAGE_HEADER_MID = "\t</title>\n"
      + "\t\t<style><!--\n"
      + "body{font-family:arial,sans-serif;margin-left:2em}"
      + "A.l:link{color:#6f6f6f}" + "A.u:link{color:green}"
      + ".subg{background-color:#e2f4f7}"
      + ".fwf{font-family:monospace;white-space:pre-wrap}" + "//--></style>\n";

  private static final String PAGE_HEADER_END_BODY_START = "</head>\n"
      + "<body text=#000000 bgcolor=#ffffff>\n"
      + "\t<table border=0 cellpadding=2 cellspacing=0 width=100%>\n"
      + "\t\t<tr><td rowspan=3 width=1% nowrap><b>"
      + "<font color=#c71a32 size=10>T</font>"
      + "<font color=#00a189 size=10>S</font>"
      + "<font color=#1a65b7 size=10>D</font>"
      + "&nbsp;&nbsp;</b><td>&nbsp;</td></tr>\n"
      + "\t\t<tr><td class=subg><font color=#507e9b><b>";

  private static final String PAGE_BODY_MID = "</b></td></tr>\n"
      + "\t\t<tr><td>&nbsp;</td></tr>\n\t</table>\n";

  private static final String PAGE_FOOTER = "\t<table width=100% cellpadding=0 cellspacing=0>\n"
      + "\t<tr><td class=subg><img alt=\"\" width=1 height=6></td></tr>\n"
      + "\t</table>\n</body>\n</html>";

  private static final int BOILERPLATE_LENGTH = PAGE_HEADER_START.length()
      + PAGE_HEADER_MID.length() + PAGE_HEADER_END_BODY_START.length()
      + PAGE_BODY_MID.length() + PAGE_FOOTER.length();

  /** Precomputed 404 page. */
  private static final StringBuilder PAGE_NOT_FOUND = makePage(
      "Page Not Found", "Error 404", "<blockquote>" + "<h1>Page Not Found</h1>"
          + "The requested URL was not found on this server." + "</blockquote>");
}
