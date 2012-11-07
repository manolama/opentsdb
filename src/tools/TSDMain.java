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

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.hbase.async.HBaseClient;

import net.opentsdb.BuildData;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TsdbConfig;
import net.opentsdb.core.TSDB.TSDRole;
import net.opentsdb.formatters.TSDFormatter;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.TsdbStoreCass;
import net.opentsdb.storage.TsdbStoreHBase;
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
    TsdbConfig config = null;
    
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
        "Maximum time for which a new data point can be buffered.");
    CliOptions.addAutoMetricFlag(argp);
    CliOptions.addVerbose(argp);
    args = CliOptions.parse(argp, args);
    
    // load config if the user specified one
    final String config_file = argp.get("--configfile", "");
    if (!config_file.isEmpty())
      config = new TsdbConfig(config_file);
    else
      config= new TsdbConfig();
    
    if ((config.httpStaticRoot().isEmpty() || config.cacheDirectory().isEmpty())
        && (args == null || !argp.has("--port") || !argp.has("--staticroot") || !argp
            .has("--cachedir"))) {
      usage(argp, "Invalid usage. Missing required arguments", 1);
    } else if (args.length != 0) {
      usage(argp, "Too many arguments.", 2);
    }
    args = null; // free().

    // load CLI overloads
    argp.overloadConfigs(config);
    
    // dump the configuration 
    log.debug(config.dumpConfiguration(false));

    // check to make sure the directories are read/writable where appropriate
    String error = checkDirectory(
        config.httpStaticRoot(), DONT_CREATE,
        !MUST_BE_WRITEABLE);
    if (!error.isEmpty())
      usage(argp, "[tsd.staticroot] " + error, 3);
    error = checkDirectory(config.cacheDirectory(),
        CREATE_IF_NEEDED, MUST_BE_WRITEABLE);
    if (!error.isEmpty())
      usage(argp, "[tsd.cachdir] " + error, 3);

    // setup some threads
    final NioServerSocketChannelFactory factory = new NioServerSocketChannelFactory(
        Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
    
    // setup hbase client
    //final HBaseClient client = CliOptions.clientFromOptions(config);
    log.info("Setup the HBase client");
    try {
      // Make sure we don't even start if we can't find out tables.
//      client.ensureTableExists(config.tsdTable()).joinUninterruptibly();
//      client.ensureTableExists(config.tsdUIDTable()).joinUninterruptibly();
//
//      client.setFlushInterval((short)config.flushInterval());

      final TsdbStore uid_storage;
      final TsdbStore data_storage;
      final Boolean use_cass = true;
      if (use_cass){
        config.setConfig("tsd.storage.table.uid", "tsdbuid");
        // temp!
        log.info("Running with Casandra");
//        config.tsdUIDTable("tsdbuid");
//        config.tsdTable("tsdb");
        uid_storage = new TsdbStoreCass(config, config.tsdUIDTable().getBytes());
        data_storage = new TsdbStoreCass(config, config.tsdTable().getBytes());
        data_storage.setTable("tsdb");
      }else{
        log.info("Running with HBase");
        uid_storage = new TsdbStoreHBase(config, config.tsdUIDTable().getBytes());
        data_storage = new TsdbStoreHBase(config, config.tsdTable().getBytes());
      }
      final TSDB tsdb = new TSDB(uid_storage, data_storage, config);
      log.info("Setup tsdb");   
      registerShutdownHook(tsdb);
      log.info("Registered shutdown hook");
      
      if (!TSDFormatter.initClassMap(tsdb)){
        log.error("Unable to initialize the formatter map");
        System.exit(1);
      }
      
      // load the tsuid hashes first!
      if (tsdb.role == TSDRole.Ingest)
        tsdb.ts_uids.loadAllHashes();
//      log.info("Loaded all hashes, shutting down");
//      tsdb.shutdown().joinUninterruptibly();
//      if (true)
//        return;
      
      tsdb.startManagementThreads();
      final ServerBootstrap server = new ServerBootstrap(factory);

      // setup the network sockets
      server.setPipelineFactory(new PipelineFactory(tsdb));
      server.setOption("child.tcpNoDelay", config.networkTCPNoDelay());
      server.setOption("child.keepAlive", config.networkKeepalive());
      server.setOption("reuseAddress", config.networkReuseAddress());
      final InetSocketAddress addr = new InetSocketAddress(
          config.networkPort());
      
      // start the server
      server.bind(addr);
      
      log.info("Ready to serve on " + addr);
    } catch (Throwable e) {
      factory.releaseExternalResources();
      try {
        //tsdb.shutdown().joinUninterruptibly();
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
