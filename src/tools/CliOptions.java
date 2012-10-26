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
package net.opentsdb.tools;

import java.util.HashMap;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

import net.opentsdb.core.TsdbConfig;

import org.slf4j.LoggerFactory;

import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;

import org.hbase.async.HBaseClient;

/** Helper functions to parse arguments passed to {@code main}. */
final class CliOptions {

  static {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
  }

  /** This maps CLI options to their Configuration option settings */
  public static final HashMap<String, String> CliToConfig;
  static {
    HashMap<String, String> aMap = new HashMap<String, String>();
    aMap.put("--table", "tsd.table");
    aMap.put("--uidtable", "tsd.uidtable");
    aMap.put("--zkquorum", "tsd.zkquorum");
    aMap.put("--zkbasedir", "tsd.zkbasedir");
    aMap.put("--logconfig", "tsd.logconfig");
    aMap.put("--port", "tsd.network.port");
    aMap.put("--staticroot", "tsd.staticroot");
    aMap.put("--cachedir", "tsd.cachedir");
    aMap.put("--flush-interval", "tsd.flush.interval");
    aMap.put("--auto-metric", "tsd.autometric");
    CliToConfig = aMap;
  }

  /** Adds common TSDB options to the given {@code argp}. */
  static void addCommon(final ArgP argp) {
    argp.addOption("--table", "TABLE",
        "Name of the HBase table where to store the time series"
            + " (default: tsdb).");
    argp.addOption("--uidtable", "TABLE",
        "Name of the HBase table to use for Unique IDs"
            + " (default: tsdb-uid).");
    argp.addOption("--zkquorum", "SPEC",
        "Specification of the ZooKeeper quorum to use"
            + " (default: localhost).");
    argp.addOption("--zkbasedir", "PATH",
        "Path under which is the znode for the -ROOT- region"
            + " (default: /hbase).");
    argp.addOption("--configfile", "PATH", "Path to a configuration file"
        + " (defaults: /etc/opentsdb/opentsdb.conf or ./opentsdb.conf).");
    argp.addOption("--logconfig", "PATH",
        "Path to a LOGBACK configuration file"
            + " (defaults: {classpath}/logback.groovy or logback.xml).");
  }

  /** Adds a --verbose flag. */
  static void addVerbose(final ArgP argp) {
    argp.addOption("--verbose",
        "Print more logging messages and not just errors.");
    argp.addOption("-v", "Short for --verbose.");
  }

  /** Adds the --auto-metric flag. */
  static void addAutoMetricFlag(final ArgP argp) {
    argp.addOption("--auto-metric", "Automatically add metrics to tsdb as they"
        + " are inserted.  Warning: this may cause unexpected"
        + " metrics to be tracked");
  }

  /**
   * Parse the command line arguments with the given options.
   * @param options Options to parse in the given args.
   * @param args Command line arguments to parse.
   * @return The remainder of the command line or {@code null} if {@code args}
   *         were invalid and couldn't be parsed.
   */
  static String[] parse(final ArgP argp, String[] args) {
    try {
      args = argp.parse(args);
    } catch (IllegalArgumentException e) {
      System.err.println("Invalid usage.  " + e.getMessage());
      return null;
    }
    honorVerboseFlag(argp);
    return args;
  }

  /** Changes the log level to 'WARN' unless --verbose is passed. */
  private static void honorVerboseFlag(final ArgP argp) {
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
 
    // uncomment to dump it's config to stdout, you can use this to see what
    // config file it loaded
    //StatusPrinter.print(lc);
    
    // this is a gross hack to determine if a config file was loaded. If one was
    // loaded, the count should be greater than 4 or 5, the default count when 
    // the app launches
    if (lc.getStatusManager().getCount() >= 5)
      return;
    
    if (argp.optionExists("--verbose") && !argp.has("--verbose")
        && !argp.has("-v")) {
      // SLF4J doesn't provide any API to programmatically set the logging
      // level of the underlying logging library. So we have to violate the
      // encapsulation provided by SLF4J.
      for (final Logger logger : ((Logger) LoggerFactory
          .getLogger(Logger.ROOT_LOGGER_NAME)).getLoggerContext()
          .getLoggerList()) {
        logger.setLevel(Level.WARN);
      }
    }
  }

  /**
   * Creates a new HBaseClient object using settings from the 
   * configuration instance
   * @param config Configuration instance to use
   * @return A reference to an HBaseClient
   */
  static HBaseClient clientFromOptions(final TsdbConfig config) {
    final String zkbasedir = config.zookeeperBaseDirectory();

    if (zkbasedir.isEmpty())
      return new HBaseClient(config.zookeeperQuorum());
    else
      return new HBaseClient(config.zookeeperQuorum(), zkbasedir);
  }

}
