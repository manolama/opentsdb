package net.opentsdb.storage;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;

import com.google.api.client.util.Lists;
import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.cloud.bigtable.util.ByteStringer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import net.opentsdb.core.MockTSDB;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Bytes;

public class TestTsdb1xBigtableDataStore {

  @Test
  public void foo() throws Exception {
    Tsdb1xBigtableFactory factory = mock(Tsdb1xBigtableFactory.class);
    MockTSDB tsdb = new MockTSDB();
    
    tsdb.config.register(Tsdb1xBigtableDataStore.PROJECT_ID_KEY, 
        "calcium-post-108621", false, "UT");
    tsdb.config.register(Tsdb1xBigtableDataStore.INSTANCE_ID_KEY, 
        "opentsdb", false, "UT");
    tsdb.config.register(Tsdb1xBigtableDataStore.ZONE_ID_KEY, 
        "us-central1-c", false, "UT");
    tsdb.config.register(Tsdb1xBigtableDataStore.JSON_KEYFILE_KEY, 
        "/Users/clarsen/Documents/opentsdb/bigtable/quickstart/key.json", false, "UT");
    when(factory.tsdb()).thenReturn(tsdb);
    
    Tsdb1xBigtableDataStore store = new Tsdb1xBigtableDataStore(factory, null, 
        mock(Schema.class));
    
//    ReadRowsRequest req = ReadRowsRequest.newBuilder()
//        .setTableNameBytes(ByteString.copyFrom(store.uid_table))
//        .setRowsLimit(42)
//        .setFilter(RowFilter.newBuilder()
//            .setFamilyNameRegexFilterBytes(ByteStringer.wrap("name".getBytes()))
//            .setColumnQualifierRegexFilter(ByteStringer.wrap("metrics".getBytes()))
////            .setColumnRangeFilter(ColumnRange.newBuilder()
////                .setFamilyName("id")
////                .setStartQualifierOpen(ByteStringer.wrap("metrics".getBytes()))
////                .setEndQualifierOpen(ByteStringer.wrap("metrics".getBytes())))
//            )
//        .buildPartial();
//    
//    ListenableFuture<List<Row>> future = store.session.createAsyncExecutor().readRowsAsync(req);
//    //System.out.println(future.get());
//    for (final Row r : future.get()) {
//      System.out.println(Bytes.pretty(r.getKey().toByteArray()));
//    }
    
//    List<byte[]> uids = Lists.newArrayList();
//    uids.add(new byte[] { 0, 6, -13 });
//    uids.add(new byte[] { 99, 99, 99 });
//    uids.add(new byte[] { 0, 6, -10 });
//    
//    System.out.println(store.uid_store.getNames(UniqueIdType.METRIC, 
//        uids,
//        null).join());
    
  ReadRowsRequest req = ReadRowsRequest.newBuilder()
  .setTableNameBytes(ByteString.copyFrom(store.uid_table))
  //.setRowsLimit(10)
  .setFilter(RowFilter.newBuilder()
      .setFamilyNameRegexFilterBytes(ByteStringer.wrap("name".getBytes()))
      .setColumnQualifierRegexFilter(ByteStringer.wrap("metrics".getBytes()))
      .setCellsPerColumnLimitFilter(1)
//      .setColumnRangeFilter(ColumnRange.newBuilder()
//          .setFamilyName("id")
//          .setStartQualifierOpen(ByteStringer.wrap("metrics".getBytes()))
//          .setEndQualifierOpen(ByteStringer.wrap("metrics".getBytes())))
      )
  .buildPartial();

    ResultScanner<FlatRow> scnr = store.session.getDataClient().readFlatRows(req);
    FlatRow[] rows = scnr.next(10);
    
    while (rows != null && rows.length > 0) {
      System.out.println(rows.length);
      rows = scnr.next(10);
    }

  
    store.session.close();
    //Thread.sleep(100000);
  }
}
