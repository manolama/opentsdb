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

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import org.hbase.async.HBaseClient;

import net.opentsdb.BuildData;
import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Configuration;
import net.opentsdb.tsd.PipelineFactory;

/**
 * Main class of the TSD, the Time Series Daemon.
 */
final class TSDMain {

  /**
   * Prints the command line usage information and exits with a specified
   * exit code
   * @param argp The list of command line arguments to print
   * @param errmsg An error message to display to the user before printing
   * the list of options
   * @param retval The numeric exit code to return
   */
  static void usage(final ArgP argp, final String errmsg, final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: tsd --port=PORT"
        + " --staticroot=PATH --cachedir=PATH\n"
        + "Starts the TSD, the Time Series Daemon");
    if (argp != null) {
      System.err.print(argp.usage());
    }
    System.exit(retval);
  }

  /** Used to determine if we need to create a direcotry */
  private static final boolean DONT_CREATE = false;
  private static final boolean CREATE_IF_NEEDED = true;
  private static final boolean MUST_BE_WRITEABLE = true;

  /**
   * Ensures the given directory path is usable and set it as a system prop. In
   * case of problem, this function calls {@code System.exit}.
   * @param prop The name of the system property to set.
   * @param dir The path to the directory that needs to be checked.
   * @param need_write Whether or not the directory must be writeable.
   * @param create If {@code true}, the directory {@code dir} will be created if
   *          it doesn't exist.
   */
  private static String checkDirectory(final String dir,
      final boolean need_write, final boolean create) {
    if (dir.isEmpty())
      return "Directory path is empty";
    final File f = new File(dir);
    if (!f.exists() && !(create && f.mkdirs())) {
      return "No such directory [" + dir + "]";
    } else if (!f.isDirectory()) {
      return "Not a directory [" + dir + "]";
    } else if (need_write && !f.canWrite()) {
      return "Cannot write to directory [" + dir + "]";
    }
    return "";
  }

  /**
   * The main entry point for the Time Series Daemon
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    Logger log = LoggerFactory.getLogger(TSDMain.class);
    log.info("Starting.");
    log.info(BuildData.revisionString());
    log.info(BuildData.buildString());

    // try to load default config
    final boolean loaded_config = Configuration.loadConfig();

    try {
      System.in.close(); // Release a FD we don't need.
    } catch (Exception e) {
      log.warn("Failed to close stdin", e);
    }

    // load CLI options
    final ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--port", "NUM", "TCP port to listen on.");
    argp.addOption("--staticroot", "PATH",
        "Web root from which to serve static files (/s URLs).");
    argp.addOption("--cachedir", "PATH",
        "Directory under which to cache result of requests.");
    argp.addOption("--flush-interval", "MSEC",
        "Maximum time for which a new data point can be buffered"
            + " (default: " + Const.FLUSH_INTERVAL + ").");
    CliOptions.addAutoMetricFlag(argp);
    args = CliOptions.parse(argp, args);
    if (!loaded_config
        && (args == null || !argp.has("--port") || !argp.has("--staticroot") || !argp
            .has("--cachedir"))) {
      usage(argp, "Invalid usage.", 1);
    } else if (args.length != 0) {
      usage(argp, "Too many arguments.", 2);
    }
    args = null; // free().

    // load config if the user specified one
    final String config_file = argp.get("--configfile", "");
    if (!config_file.isEmpty())
      Configuration.loadConfig(config_file);

    // load CLI overloads
    argp.overloadConfigs();
    
    // dump the configuration 
    log.debug(Configuration.dumpConfiguration(false));

    // check to make sure the directories are read/writable where appropriate
    String error = checkDirectory(
        Configuration.getString("tsd.staticroot", ""), DONT_CREATE,
        !MUST_BE_WRITEABLE);
    if (!error.isEmpty())
      usage(argp, "[tsd.staticroot] " + error, 3);
    error = checkDirectory(Configuration.getString("tsd.cachedir", ""),
        CREATE_IF_NEEDED, MUST_BE_WRITEABLE);
    if (!error.isEmpty())
      usage(argp, "[tsd.cachdir] " + error, 3);

    // setup some threads
    final NioServerSocketChannelFactory factory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    
    // setup hbase client
    final HBaseClient client = CliOptions.clientFromOptions(argp);
    
    try {
      // Make sure we don't even start if we can't find out tables.
      final String table = Configuration.getString("tsd.table",
          Const.HBASE_TABLE);
      final String uidtable = Configuration.getString("tsd.uidtable",
          Const.HBASE_UIDTABLE);
      client.ensureTableExists(table).joinUninterruptibly();
      client.ensureTableExists(uidtable).joinUninterruptibly();

      client.setFlushInterval(Configuration.getShort("tsd.flush.interval",
          Const.FLUSH_INTERVAL, (short) 1, Short.MAX_VALUE));

      final TSDB tsdb = new TSDB(client, table, uidtable);
      registerShutdownHook(tsdb);
      final ServerBootstrap server = new ServerBootstrap(factory);

      // setup the network sockets
      server.setPipelineFactory(new PipelineFactory(tsdb));
      server.setOption("child.tcpNoDelay", Configuration.getBoolean(
          "tsd.network.tcpnodelay", Const.NETWORK_TCP_NODELAY));
      server.setOption("child.keepAlive", Configuration.getBoolean(
          "tsd.network.keepalive", Const.NETWORK_KEEPALIVE));
      server.setOption("reuseAddress", Configuration.getBoolean(
          "tsd.network.reuseaddress", Const.NETWORK_REUSEADDRESS));
      final InetSocketAddress addr = new InetSocketAddress(
          Configuration.getInt("tsd.network.port", Const.NETWORK_PORT));
      
      // start the server
      server.bind(addr);
      
      log.info("Ready to serve on " + addr);
    } catch (Throwable e) {
      factory.releaseExternalResources();
      try {
        client.shutdown().joinUninterruptibly();
      } catch (Exception e2) {
        log.error("Failed to shutdown HBase client", e2);
      }
      throw new RuntimeException("Initialization failed", e);
    }
    // The server is now running in separate threads, we can exit main.
  }

  /**
   * ?
   * @param tsdb The TSDB object to work with
   */
  private static void registerShutdownHook(final TSDB tsdb) {
    final class TSDBShutdown extends Thread {
      public TSDBShutdown() {
        super("TSDBShutdown");
      }

      public void run() {
        try {
          tsdb.shutdown().join();
        } catch (Exception e) {
          LoggerFactory.getLogger(TSDBShutdown.class).error(
              "Uncaught exception during shutdown", e);
        }
      }
    }
    Runtime.getRuntime().addShutdownHook(new TSDBShutdown());
  }

}
