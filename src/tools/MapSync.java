// This file is part of OpenTSDB.
// Copyright (C) 2011  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.core.Config;
import net.opentsdb.core.Const;
import net.opentsdb.core.Internal;
import net.opentsdb.core.TSDB;
import net.opentsdb.storage.TsdbScanner;
import net.opentsdb.storage.TsdbStorageException;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.storage.TsdbStoreHBase;
import net.opentsdb.uid.TimeseriesUID;
import net.opentsdb.uid.UniqueId;

import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Walks the entire TSDB table and synchronizes the timeseries UID table as well
 * as UID object maps. You can run this as often as you want and it won't harm
 * the system.
 * 
 * NOTE: Since it does walk the *entire* table, if you have a ton of data, this
 * could take a LONG time to complete.
 */
final class MapSync {
  private static final Logger LOG = LoggerFactory.getLogger(MapSync.class);

  /** Prints usage and exits with the given retval. */
  private static void usage(final ArgP argp, final String errmsg,
      final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: mapsync");
    System.err.print(argp.usage());
    System.exit(retval);
  }

  /**
   * Does all of the work
   * @param args CLI arguments
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    CliOptions.addVerbose(argp);
    args = CliOptions.parse(argp, args);
    if (args == null) {
      usage(argp, "Invalid usage.", 1);
    }/*else if (args.length < 1) {
      usage(argp, String.format("Not enough arguments [%d]", args.length), 2);
    }*/    
    // ^^^^ dunno why that's not working properly

    // TODO instantiate config properly
    Config config = new Config();

    // load config if the user specified one
    final String config_file = argp.get("--configfile", "");
    if (!config_file.isEmpty())
      config.loadConfig(config_file);
    else
      config.loadConfig();

    // load CLI overloads
    argp.overloadConfigs(config);

    // dump the configuration
    LOG.debug(config.dumpConfiguration(false));

    // setup hbase client
    final HBaseClient client = CliOptions.clientFromOptions(config);
    final TsdbStore uid_storage = new TsdbStoreHBase(config, config.tsdUIDTable().getBytes(), client);
    final TsdbStore data_storage = new TsdbStoreHBase(config, config.tsdTable().getBytes(), client);
    final TSDB tsdb = new TSDB(uid_storage, data_storage, config);
    argp = null;
    try {
      
      // TEMP ---------------     
      cellKiller(uid_storage, "id", "ts_uids");
      cellKiller(uid_storage, "id", "metrics_map");
      cellKiller(uid_storage, "id", "tagk_map");
      cellKiller(uid_storage, "id", "tagv_map");
      cellKiller(uid_storage, "name", "name_meta");
      cellKiller(uid_storage, "name", "metrics_meta");
      cellKiller(uid_storage, "name", "tagk_meta");
      cellKiller(uid_storage, "name", "tagv_meta");
      cellKiller(uid_storage, "name", "ts_meta");
      System.exit(0);
      
      TsdbScanner scanner = new TsdbScanner();
      scanner.setFamily(TsdbStore.toBytes("t"));
      scanner = data_storage.openScanner(scanner);
      long rowcount = 0;
      final short metric_width = Internal.metricWidth(tsdb);
      
      ArrayList<ArrayList<KeyValue>> rows;
      String last_key = "";
      long last_time = 0;
      while ((rows = data_storage.nextRows(scanner).joinUninterruptibly()) != null) {
        //LOG.debug("Processing next set of rows");
        for (final ArrayList<KeyValue> row : rows) {
          rowcount++;
          
          // Take a copy of the row-key because we're going to zero-out the
          // timestamp and use that as a key in our `seen' map.
          final byte[] temp = row.get(0).key().clone();
          //System.out.println(UniqueId.IDtoString(temp));
          // we can use base time for the record time
          final long base_time = Bytes.getUnsignedInt(temp, metric_width);
          
          int x=0;
          String ts_uid = UniqueId.IDtoString(TimeseriesUID.getTSUIDFromKey(temp, (short)3, (short)4));
          
          // store in the ts_uids if it's different
          if (last_key == "" || !last_key.equals(ts_uid)){
            //System.out.println(String.format("New key [%s]", str_key));
            tsdb.ts_uids.add(ts_uid);
            
            // update maps and metadata
            //tsdb.processNewTSUID(ts_uid, false);
            
            // update metadata
            
            
            // update
            last_key = ts_uid;
          }
//          if (rowcount > 100)
//            break;
        }
//        if (rowcount > 100)
//          break;
      }
      LOG.info(String.format("Finished processing TSDB table with [%d] rows", rowcount));
      
      // flush pending changes
      tsdb.flush().joinUninterruptibly();
      LOG.info("Completed the flush");
    } catch (Exception e){
      e.printStackTrace();
    }finally {
      LOG.info("Shutting down Storage client");
      tsdb.shutdown().joinUninterruptibly();
    }    
    LOG.info("All done ------");
    System.exit(0);
  }

  private static void cellKiller(final TsdbStore storage, final String cf, final String qualifier){
    storage.setTable("tsdb-uid");
    TsdbScanner scanner = new TsdbScanner();
//    scanner.setStartRow(new byte[] {0, 0, 0});
//    scanner.setEndRow(new byte[] {127, 127, 127});
    scanner.setFamily(TsdbStore.toBytes(cf));
    scanner.setQualifier(TsdbStore.toBytes(qualifier));
    scanner = storage.openScanner(scanner);
    
    long rowcount=0;
    ArrayList<ArrayList<KeyValue>> rows;
    try {
      while ((rows = storage.nextRows(scanner).joinUninterruptibly()) != null) {
        for (final ArrayList<KeyValue> row : rows) {
          final byte[] key = row.get(0).key().clone();
          storage.deleteValue(key, TsdbStore.toBytes(cf), TsdbStore.toBytes(qualifier));
          rowcount++;
        }
      }
    } catch (TsdbStorageException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    System.out.println(String.format("Deleted [%d] cells", rowcount));
  }
}
