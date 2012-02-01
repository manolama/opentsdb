// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.graph;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.core.Const;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;

/**
 * Produces files to generate graphs with Gnuplot.
 * <p>
 * This class takes a bunch of {@link DataPoints} instances and generates a
 * Gnuplot script as well as the corresponding data files to feed to Gnuplot.
 */
public final class Plot {

  private static final Logger LOG = LoggerFactory.getLogger(Plot.class);

  /** Start time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private int start_time;

  /** End time (UNIX timestamp in seconds) on 32 bits ("unsigned" int). */
  private int end_time;

  /** All the DataPoints we want to plot. */
  private ArrayList<DataPoints> datapoints =
    new ArrayList<DataPoints>();

  /** Per-DataPoints Gnuplot options. */
  private ArrayList<String> options = new ArrayList<String>();

  /** Global Gnuplot parameters. */
  private Map<String, String> params = new HashMap<String, String>();

  /** Minimum width / height allowed. */
  private static final short MIN_PIXELS = 100;

  /** Width of the graph to generate, in pixels. */
  private short width = (short) 1024;

  /** Height of the graph to generate, in pixels. */
  private short height = (short) 768;

  /**
   * Number of seconds of difference to apply in order to get local time.
   * Gnuplot always renders timestamps in UTC, so we simply apply a delta
   * to get local time.  If the local time changes (e.g. due to DST changes)
   * we won't pick up the change unless we restart.
   * TODO(tsuna): Do we want to recompute the offset every day to avoid this
   * problem?
   */
  private static final int utc_offset =
    TimeZone.getDefault().getOffset(System.currentTimeMillis()) / 1000;

  /**
   * Constructor.
   * @param start_time Timestamp of the start time of the graph.
   * @param end_time Timestamp of the end time of the graph.
   * @throws IllegalArgumentException if either timestamp is 0 or negative.
   * @throws IllegalArgumentException if {@code start_time >= end_time}.
   */
  public Plot(final long start_time, final long end_time) {
    if ((start_time & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid start time: " + start_time);
    } else if ((end_time & 0xFFFFFFFF00000000L) != 0) {
      throw new IllegalArgumentException("Invalid end time: " + end_time);
    } else if (start_time >= end_time) {
      throw new IllegalArgumentException("start time (" + start_time
        + ") is greater than or equal to end time: " + end_time);
    }
    this.start_time = (int) start_time;
    this.end_time = (int) end_time;
  }

  /**
   * Sets the global parameters for this plot.
   * @param params Each entry is a Gnuplot setting that will be written as-is
   * in the Gnuplot script file: {@code set KEY VALUE}.
   * When the value is {@code null} the script will instead contain
   * {@code unset KEY}.
   */
  public void setParams(final Map<String, String> params) {
    this.params = params;
  }

  /**
   * Sets the dimensions of the graph (in pixels).
   * @param width The width of the graph produced (in pixels).
   * @param height The height of the graph produced (in pixels).
   * @throws IllegalArgumentException if the width or height are negative,
   * zero or "too small" (e.g. less than 100x100 pixels).
   */
  public void setDimensions(final short width, final short height) {
    if (width < MIN_PIXELS || height < MIN_PIXELS) {
      final String what = width < MIN_PIXELS ? "width" : "height";
      throw new IllegalArgumentException(what + " smaller than " + MIN_PIXELS
                                         + " in " + width + 'x' + height);
    }
    this.width = width;
    this.height = height;
  }

  /**
   * Adds some data points to this plot.
   * @param datapoints The data points to plot.
   * @param options The options to apply to this specific series.
   */
  public void add(final DataPoints datapoints,
                  final String options) {
    // Technically, we could check the number of data points in the
    // datapoints argument in order to do something when there are none, but
    // this is potentially expensive with a SpanGroup since it requires
    // iterating through the entire SpanGroup.  We'll check this later
    // when we're trying to use the data, in order to avoid multiple passes
    // through the entire data.
    this.datapoints.add(datapoints);
    this.options.add(options);
  }

  /**
   * Returns a view on the datapoints in this plot.
   * Do not attempt to modify the return value.
   */
  public Iterable<DataPoints> getDataPoints() {
    return datapoints;
  }

  /**
   * Applies the plot parameters from the query to the given plot.
   * @param query The query from which to get the query string.
   * @param plot The plot on which to apply the parameters.
   */
  public void setPlotParams(final Map<String, List<String>> querystring) {
    String value;
    if ((value = popParam(querystring, "yrange")) != null) {
      params.put("yrange", value);
    }
    if ((value = popParam(querystring, "y2range")) != null) {
      params.put("y2range", value);
    }
    if ((value = popParam(querystring, "ylabel")) != null) {
      params.put("ylabel", stringify(value));
    }
    if ((value = popParam(querystring, "y2label")) != null) {
      params.put("y2label", stringify(value));
    }
    if ((value = popParam(querystring, "yformat")) != null) {
      params.put("format y", stringify(value));
    }
    if ((value = popParam(querystring, "y2format")) != null) {
      params.put("format y2", stringify(value));
    }
    if ((value = popParam(querystring, "xformat")) != null) {
      params.put("format x", stringify(value));
    }
    if ((value = popParam(querystring, "ylog")) != null) {
      params.put("logscale", "y");
    }
    if ((value = popParam(querystring, "y2log")) != null) {
      params.put("logscale", "y2");
    }
    if ((value = popParam(querystring, "key")) != null) {
      params.put("key", value);
    }
    if ((value = popParam(querystring, "title")) != null) {
      params.put("title", stringify(value));
    }
    // This must remain after the previous `if' in order to properly override
    // any previous `key' parameter if a `nokey' parameter is given.
    if ((value = popParam(querystring, "nokey")) != null) {
      params.put("key", null);
    }
    
    String wxh = "";
    // get the dimensions
    if ((value = popParam(querystring, "wxh")) != null){
      wxh = value;
    }
    try {
      this.setPlotDimensions(wxh);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  /**
   * Generates the Gnuplot script and data files.
   * @param basepath The base path to use.  A number of new files will be
   * created and their names will all start with this string.
   * @return The number of data points sent to Gnuplot.  This can be less
   * than the number of data points involved in the query due to things like
   * aggregation or downsampling.
   * @throws IOException if there was an error while writing one of the files.
   */
  public int dumpToFiles(final String basepath) throws IOException {
    int npoints = 0;
    final int nseries = datapoints.size();
    final String datafiles[] = nseries > 0 ? new String[nseries] : null;
    for (int i = 0; i < nseries; i++) {
      datafiles[i] = basepath + "_" + i + ".dat";
      final PrintWriter datafile = new PrintWriter(datafiles[i]);
      try {
        for (final DataPoint d : datapoints.get(i)) {
          final long ts = d.timestamp();
          if (ts >= start_time && ts <= end_time) {
            npoints++;
          }
          datafile.print(ts + utc_offset);
          datafile.print(' ');
          if (d.isInteger()) {
            datafile.print(d.longValue());
          } else {
            final double value = d.doubleValue();
            if (value != value || Double.isInfinite(value)) {
              throw new IllegalStateException("NaN or Infinity found in"
                  + " datapoints #" + i + ": " + value + " d=" + d);
            }
            datafile.print(value);
          }
          datafile.print('\n');
        }
        LOG.debug("Wrote datafile: " + datafiles[i]);
      } finally {
        datafile.close();
      }
    }

    if (npoints == 0) {
      // Gnuplot doesn't like empty graphs when xrange and yrange aren't
      // entirely defined, because it can't decide on good ranges with no
      // data.  We always set the xrange, but the yrange is supplied by the
      // user.  Let's make sure it defines a min and a max.
      params.put("yrange", "[0:10]");  // Doesn't matter what values we use.
    }
    writeGnuplotScript(basepath, datafiles);
    return npoints;
  }

  /**
   * Generates the Gnuplot script.
   * @param basepath The base path to use.
   * @param datafiles The names of the data files that need to be plotted,
   * in the order in which they ought to be plotted.  It is assumed that
   * the ith file will correspond to the ith entry in {@code datapoints}.
   * Can be {@code null} if there's no data to plot.
   */
  private void writeGnuplotScript(final String basepath,
                                  final String[] datafiles) throws IOException {
    String script_path = basepath + ".gnuplot";
    final PrintWriter gp = new PrintWriter(script_path);
    try {
      // XXX don't hardcode all those settings.  At least not like that.
      gp.append("set term png small size ")
        // Why the fuck didn't they also add methods for numbers?
        .append(Short.toString(width)).append(",")
        .append(Short.toString(height)).append("\n"
                + "set xdata time\n"
                + "set timefmt \"%s\"\n"
                + "set xtic rotate\n"
                + "set output \"").append(
                    (System.getProperty("os.name").contains("Windows")
                    ? basepath.replace("\\", "\\\\")
                        : basepath) + ".png").append("\"\n"
                + "set xrange [\"")
        .append(String.valueOf(start_time + utc_offset))
        .append("\":\"")
        .append(String.valueOf(end_time + utc_offset))
        .append("\"]\n");
      if (!params.containsKey("format x")) {
        gp.append("set format x \"").append(xFormat()).append("\"\n");
      }
      LOG.debug("Going to get datapoints");
      final int nseries = datapoints.size();
      LOG.debug("Datapoints size: " + nseries);
      if (nseries > 0) {
        gp.write("set grid\n"
                 + "set style data linespoints\n");
        if (!params.containsKey("key")) {
          gp.write("set key right box\n");
        }
      } else {
        gp.write("unset key\n");
        if (params == null || !params.containsKey("label")) {
          gp.write("set label \"No data\" at graph 0.5,0.9 center\n");
        }
      }

      if (params != null) {
        for (final Map.Entry<String, String> entry : params.entrySet()) {
          final String key = entry.getKey();
          final String value = entry.getValue();
          if (value != null) {
            gp.append("set ").append(key)
              .append(' ').append(value).write('\n');
          } else {
            gp.append("unset ").append(key).write('\n');
          }
        }
      }
      for (final String opts : options) {
        if (opts.contains("x1y2")) {
          // Create a second scale for the y-axis on the right-hand side.
          gp.write("set y2tics border\n");
          break;
        }
      }

      gp.write("plot ");
      for (int i = 0; i < nseries; i++) {
        final DataPoints dp = datapoints.get(i);
        final String title = dp.metricName() + dp.getTags();
        gp.append(" \"").append(
            System.getProperty("os.name").contains("Windows")
            ? datafiles[i].replace("\\", "\\\\")
                : datafiles[i]).append("\" using 1:2 title \"")
          // TODO(tsuna): Escape double quotes in title.
          .append(title).write('"');
        final String opts = options.get(i);
        if (!opts.isEmpty()) {
          gp.append(' ').write(opts);
        }
        if (i != nseries - 1) {
          gp.print(", \\");
        }
        gp.write('\n');
      }
      if (nseries == 0) {
        gp.write('0');
      }
    }catch (NullPointerException e){
      e.printStackTrace();
      //LOG.error();
      //LOG.error(e.getStackTrace());
    }
    finally {
      gp.close();
      LOG.info("Wrote Gnuplot script to [" + script_path + "]");
    }
  }

  /**
   * Finds some sensible default formatting for the X axis (time).
   * @return The Gnuplot time format string to use.
   */
  private String xFormat() {
    long timespan = end_time - start_time;
    if (timespan < 2100) {  // 35m
      return "%H:%M:%S";
    } else if (timespan < 86400) {  // 1d
      return "%H:%M";
    } else if (timespan < 604800) {  // 1w
      return "%a %H:%M";
    } else if (timespan < 1209600) {  // 2w
      return "%a %d %H:%M";
    } else if (timespan < 7776000) {  // 90d
      return "%b %d";
    } else {
      return "%Y/%m/%d";
    }
  }

  /** Parses the {@code wxh} query parameter to set the graph dimension. 
   * @throws Exception */
  private void setPlotDimensions(final String wxh) throws Exception {
    //final String wxh = query.getQueryStringParam("wxh");
    if (wxh != null && !wxh.isEmpty()) {
      final int wxhlength = wxh.length();
      if (wxhlength < 7) {  // 100x100 minimum.
        throw new Exception("Parameter wxh too short: " + wxh);
      }
      final int x = wxh.indexOf('x', 3);  // Start at 2 as min size is 100x100
      if (x < 0) {
        throw new Exception("Invalid wxh parameter: " + wxh);
      }
      try {
        final short width = Short.parseShort(wxh.substring(0, x));
        final short height = Short.parseShort(wxh.substring(x + 1, wxhlength));
        try {
          this.setDimensions(width, height);
        } catch (IllegalArgumentException e) {
          throw new Exception("Invalid wxh parameter: " + wxh + ", "
                                        + e.getMessage());
        }
      } catch (NumberFormatException e) {
        throw new Exception("Can't parse wxh '" + wxh + "': "
                                      + e.getMessage());
      }
    }
  }
  
  /**
   * Pops out of the query string the given parameter.
   * @param querystring The query string.
   * @param param The name of the parameter to pop out.
   * @return {@code null} if the parameter wasn't passed, otherwise the
   * value of the last occurrence of the parameter.
   */
  private String popParam(final Map<String, List<String>> querystring,
                                     final String param) {
    final List<String> params = querystring.remove(param);
    if (params == null) {
      return null;
    }
    return params.get(params.size() - 1);
  }
  
  /**
   * Formats and quotes the given string so it's a suitable Gnuplot string.
   * @param s The string to stringify.
   * @return A string suitable for use as a literal string in Gnuplot.
   */
  private String stringify(final String s) {
    final StringBuilder buf = new StringBuilder(1 + s.length() + 1);
    buf.append('"');
    final int length = s.length();
    int extra = 0;
    // First count how many extra chars we'll need, if any.
    for (int i = 0; i < length; i++) {
      final char c = s.charAt(i);
      switch (c) {
        case '"':
        case '\\':
        case '\b':
        case '\f':
        case '\n':
        case '\r':
        case '\t':
          extra++;
          continue;
      }
      if (c < 0x001F) {
        extra += 4;
      }
    }
    if (extra == 0) {
      buf.append(s);  // Nothing to escape.
      return buf.toString();
    }
    buf.ensureCapacity(buf.length() + length + extra);
    for (int i = 0; i < length; i++) {
      final char c = s.charAt(i);
      switch (c) {
        case '"':  buf.append('\\').append('"');  continue;
        case '\\': buf.append('\\').append('\\'); continue;
        case '\b': buf.append('\\').append('b');  continue;
        case '\f': buf.append('\\').append('f');  continue;
        case '\n': buf.append('\\').append('n');  continue;
        case '\r': buf.append('\\').append('r');  continue;
        case '\t': buf.append('\\').append('t');  continue;
      }
      if (c < 0x001F) {
        buf.append('\\').append('u').append('0').append('0')
          .append((char) Const.HEX[(c >>> 4) & 0x0F])
          .append((char) Const.HEX[c & 0x0F]);
      } else {
        buf.append(c);
      }
    }
    buf.append('"');
    return buf.toString();
  }
}
