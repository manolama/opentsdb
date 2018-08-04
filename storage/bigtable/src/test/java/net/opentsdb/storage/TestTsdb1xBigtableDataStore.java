//package net.opentsdb.storage;
//
//import static org.mockito.Matchers.any;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//import java.util.Arrays;
//import java.util.List;
//
//import org.junit.Test;
//
//import com.google.api.client.util.Lists;
//import com.google.bigtable.v2.CheckAndMutateRowRequest;
//import com.google.bigtable.v2.CheckAndMutateRowResponse;
//import com.google.bigtable.v2.ColumnRange;
//import com.google.bigtable.v2.MutateRowRequest;
//import com.google.bigtable.v2.Mutation;
//import com.google.bigtable.v2.ReadModifyWriteRowRequest;
//import com.google.bigtable.v2.ReadModifyWriteRowResponse;
//import com.google.bigtable.v2.ReadModifyWriteRule;
//import com.google.bigtable.v2.ReadRowsRequest;
//import com.google.bigtable.v2.Row;
//import com.google.bigtable.v2.RowFilter;
//import com.google.bigtable.v2.RowRange;
//import com.google.bigtable.v2.RowSet;
//import com.google.bigtable.v2.ValueRange;
//import com.google.bigtable.v2.Mutation.SetCell;
//import com.google.bigtable.v2.RowFilter.Chain;
//import com.google.bigtable.v2.RowFilter.Condition;
//import com.google.cloud.bigtable.data.v2.wrappers.Filters;
//import com.google.cloud.bigtable.data.v2.wrappers.Filters.ChainFilter;
//import com.google.cloud.bigtable.grpc.scanner.FlatRow;
//import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
//import com.google.cloud.bigtable.util.ByteStringer;
//import com.google.common.util.concurrent.ListenableFuture;
//import com.google.protobuf.ByteString;
//
//import net.opentsdb.core.MockTSDB;
//import net.opentsdb.data.TimeSeriesDatumId;
//import net.opentsdb.storage.schemas.tsdb1x.Schema;
//import net.opentsdb.uid.UniqueIdType;
//import net.opentsdb.utils.Bytes;
//
//public class TestTsdb1xBigtableDataStore {
//
//  @Test
//  public void foo() throws Exception {
//    Tsdb1xBigtableFactory factory = mock(Tsdb1xBigtableFactory.class);
//    MockTSDB tsdb = new MockTSDB();
//    
//    tsdb.config.register(Tsdb1xBigtableDataStore.PROJECT_ID_KEY, 
//        "calcium-post-108621", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.INSTANCE_ID_KEY, 
//        "opentsdb", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.ZONE_ID_KEY, 
//        "us-central1-c", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.JSON_KEYFILE_KEY, 
//        "/Users/clarsen/Documents/opentsdb/bigtable/quickstart/key.json", false, "UT");
//    when(factory.tsdb()).thenReturn(tsdb);
//    
//    Tsdb1xBigtableDataStore store = new Tsdb1xBigtableDataStore(factory, null, 
//        mock(Schema.class));
//    
////    ReadRowsRequest req = ReadRowsRequest.newBuilder()
////        .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
////        .setRowsLimit(42)
////        .setFilter(RowFilter.newBuilder()
////            .setFamilyNameRegexFilterBytes(ByteStringer.wrap("cf".getBytes()))
//////            .setColumnQualifierRegexFilter(ByteStringer.wrap("metrics".getBytes()))
//////            .setColumnRangeFilter(ColumnRange.newBuilder()
//////                .setFamilyName("id")
//////                .setStartQualifierOpen(ByteStringer.wrap("metrics".getBytes()))
//////                .setEndQualifierOpen(ByteStringer.wrap("metrics".getBytes())))
////            )
////        .buildPartial();
////    
////    ListenableFuture<List<Row>> future = store.session.createAsyncExecutor().readRowsAsync(req);
////    //System.out.println(future.get());
////    for (final Row r : future.get()) {
////      System.out.println(Bytes.pretty(r.getKey().toByteArray()));
////    }
//    
////    List<byte[]> uids = Lists.newArrayList();
////    uids.add(new byte[] { 0, 6, -13 });
////    uids.add(new byte[] { 99, 99, 99 });
////    uids.add(new byte[] { 0, 6, -10 });
////    
////    System.out.println(store.uid_store.getNames(UniqueIdType.METRIC, 
////        uids,
////        null).join());
//    
////  ReadRowsRequest req = ReadRowsRequest.newBuilder()
////  .setTableNameBytes(ByteString.copyFrom(store.uid_table))
////  //.setRowsLimit(10)
////  .setFilter(RowFilter.newBuilder()
////      .setFamilyNameRegexFilterBytes(ByteStringer.wrap("name".getBytes()))
////      .setColumnQualifierRegexFilter(ByteStringer.wrap("metrics".getBytes()))
////      .setCellsPerColumnLimitFilter(1)
//////      .setColumnRangeFilter(ColumnRange.newBuilder()
//////          .setFamilyName("id")
//////          .setStartQualifierOpen(ByteStringer.wrap("metrics".getBytes()))
//////          .setEndQualifierOpen(ByteStringer.wrap("metrics".getBytes())))
////      )
////  .buildPartial();
////
////    ResultScanner<FlatRow> scnr = store.session.getDataClient().readFlatRows(req);
////    FlatRow[] rows = scnr.next(10);
////    
////    while (rows != null && rows.length > 0) {
////      System.out.println(rows.length);
////      rows = scnr.next(10);
////    }
//
//    // WORKING ATOMIC INCREMENT
////    ReadModifyWriteRowRequest.Builder req = ReadModifyWriteRowRequest.newBuilder();
////    req.setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()));
////    req.setRowKey(ByteStringer.wrap(new byte[] { 1 }));
////    
////    ReadModifyWriteRule.Builder rule = ReadModifyWriteRule.newBuilder();
////    rule.setIncrementAmount(1);
////    rule.setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()));
////    rule.setColumnQualifier(ByteStringer.wrap("c".getBytes()));
////    
////    req.addRules(rule);
////    
////    ReadModifyWriteRowResponse resp = store.executor.readModifyWriteRowAsync(req.build()).get();
////    System.out.println(resp);
////    System.out.println(Bytes.getLong(resp.getRow().getFamiliesList().get(0).getColumns(0).getCellsList().get(0).getValue().toByteArray()));
//    
//  // WORKING APPENDS
////  ReadModifyWriteRowRequest.Builder req = ReadModifyWriteRowRequest.newBuilder();
////  req.setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()));
////  req.setRowKey(ByteStringer.wrap(new byte[] { 0 }));
////  
////  ReadModifyWriteRule.Builder rule = ReadModifyWriteRule.newBuilder();
////  rule.setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()));
////  rule.setColumnQualifier(ByteStringer.wrap("c".getBytes()));
////  rule.setAppendValue(ByteStringer.wrap("hi".getBytes()));
////  
////  req.addRules(rule);
////  
////  ReadModifyWriteRowResponse resp = store.executor.readModifyWriteRowAsync(req.build()).get();
//  
//    CheckAndMutateRowRequest req = CheckAndMutateRowRequest.newBuilder()
//        .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
//        .setRowKey(ByteStringer.wrap(new byte[] { 3 }))
////        .setPredicateFilter(RowFilter.newBuilder()
////            .setCellsPerColumnLimitFilter(1)
////            .setFamilyNameRegexFilter("cf")
////            .setColumnQualifierRegexFilter((ByteStringer.wrap("c".getBytes())))
////            )
//        .addTrueMutations(Mutation.newBuilder()
//            .setSetCell(SetCell.newBuilder()
//                .setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()))
//                .setColumnQualifier(ByteStringer.wrap("c".getBytes()))
//                .setValue(ByteStringer.wrap("cas".getBytes()))
//                .setTimestampMicros(System.currentTimeMillis() * 1000L)
//                )
//            )
//        .build();
//   CheckAndMutateRowResponse resp = store.executor().checkAndMutateRowAsync(req).get();
//  System.out.println("RESP: " + resp);
//  
//  ReadRowsRequest rrr = ReadRowsRequest.newBuilder()
//    .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
//    .setRowsLimit(1)
//    .setRows(RowSet.newBuilder()
//        .addRowKeys(ByteStringer.wrap(new byte[] { 0 })))
//    .setFilter(RowFilter.newBuilder()
//        .setCellsPerColumnLimitFilter(1)
//        .setFamilyNameRegexFilterBytes(ByteStringer.wrap("cf".getBytes()))
//        )
//    .buildPartial();
//
//System.out.println("GET: " + store.session.createAsyncExecutor().readRowsAsync(rrr).get());
//    
//    // WORKING PUT
////    Mutation m = Mutation.newBuilder()
////        .setSetCell(SetCell.newBuilder()
////            .setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()))
////            .setColumnQualifier(ByteStringer.wrap("c".getBytes()))
////            .setValue(ByteStringer.wrap("hi".getBytes()))
////            .setTimestampMicros(System.currentTimeMillis() * 1000L)
////            )
////        .build();
////
////    MutateRowRequest mmr = MutateRowRequest.newBuilder()
////        .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
////        .setRowKey(ByteStringer.wrap(new byte[] { 4 }))
////        .addMutations(m)
////        .build();
////    
////    System.out.println("MUTATION: " + store.executor.mutateRowAsync(mmr).get());
////    
////  ReadRowsRequest rrr = ReadRowsRequest.newBuilder()
////  .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
////  .setRowsLimit(1)
////  .setRows(RowSet.newBuilder()
////      .addRowKeys(ByteStringer.wrap(new byte[] { 4 })))
////  .setFilter(RowFilter.newBuilder()
////      .setFamilyNameRegexFilterBytes(ByteStringer.wrap("cf".getBytes()))
////      )
////  .buildPartial();
//
////System.out.println("GET: " + store.session.createAsyncExecutor().readRowsAsync(rrr).get());
//  
//  
//    
//    
//    
//    store.session.close();
//    //Thread.sleep(100000);
//  }
//  
//  @Test
//  public void cas() throws Exception {
//    Tsdb1xBigtableFactory factory = mock(Tsdb1xBigtableFactory.class);
//    MockTSDB tsdb = new MockTSDB();
//    
//    tsdb.config.register(Tsdb1xBigtableDataStore.PROJECT_ID_KEY, 
//        "calcium-post-108621", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.INSTANCE_ID_KEY, 
//        "opentsdb", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.ZONE_ID_KEY, 
//        "us-central1-c", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.JSON_KEYFILE_KEY, 
//        "/Users/clarsen/Documents/opentsdb/bigtable/quickstart/key.json", false, "UT");
//    when(factory.tsdb()).thenReturn(tsdb);
//    
//    Tsdb1xBigtableDataStore store = new Tsdb1xBigtableDataStore(factory, null, 
//        mock(Schema.class));
//    
//    
////    CheckAndMutateRowRequest req = CheckAndMutateRowRequest.newBuilder()
////        .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
////        .setRowKey(ByteStringer.wrap(new byte[] { 3 }))
////        .setPredicateFilter(RowFilter.Chain.newBuilder()
////            .addFiltersBuilder()
////            .setChain(Chain.newBuilder()
////                .addFilters(RowFilter.newBuilder()
////                    .setFamilyNameRegexFilter("cf"))
////                .addFilters(RowFilter.newBuilder()
////                    .setColumnQualifierRegexFilter((ByteStringer.wrap("c".getBytes()))))
////                .addFilters(RowFilter.newBuilder()
////                    .setValueRangeFilter(ValueRange.newBuilder()
////                        .setStartValueClosed(ByteStringer.wrap("cas".getBytes()))
////                        .setEndValueClosed(ByteStringer.wrap("cas".getBytes()))))
////                )
////            
//////            .setCellsPerColumnLimitFilter(1)
//////            .setFamilyNameRegexFilter("cf")
//////            .setColumnQualifierRegexFilter((ByteStringer.wrap("c".getBytes())))
//////            .setValueRangeFilter(ValueRange.newBuilder()
//////                .setStartValueClosed(ByteStringer.wrap("cas".getBytes()))
//////                .setEndValueClosed(ByteStringer.wrap("cas".getBytes())))
////            )
////        .addTrueMutations(Mutation.newBuilder()
////            .setSetCell(SetCell.newBuilder()
////                .setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()))
////                .setColumnQualifier(ByteStringer.wrap("c".getBytes()))
////                .setValue(ByteStringer.wrap("cas3".getBytes()))
////                )
////            )
////        .build();
//    
////    CheckAndMutateRowRequest req = CheckAndMutateRowRequest.newBuilder()
////        .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
////        .setRowKey(ByteStringer.wrap(new byte[] { 3 }))
////        .setPredicateFilter(RowFilter.Chain.newBuilder()
////            .addFiltersBuilder()
////            .setChain(Chain.newBuilder()
////                .addFilters(RowFilter.Chain.newBuilder()
////                    .addFiltersBuilder()
////                    .setChain(Chain.newBuilder()
////                        .addFilters(RowFilter.newBuilder()
////                            .setFamilyNameRegexFilterBytes(ByteStringer.wrap("cf".getBytes())))
////                        .addFilters(RowFilter.newBuilder()
////                            .setColumnQualifierRegexFilter(ByteStringer.wrap("c".getBytes()))))
////                    )
////                .addFilters(RowFilter.newBuilder()
////                    .setCellsPerColumnLimitFilter(1))
////                )
////            )
////        .addFalseMutations(Mutation.newBuilder()
////            .setSetCell(SetCell.newBuilder()
////                .setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()))
////                .setColumnQualifier(ByteStringer.wrap("c".getBytes()))
////                .setValue(ByteStringer.wrap("cas4".getBytes()))
////                .setTimestampMicros(-1)
////                )
////            )
////        .build();
//    
//    // AAAAAnd it all has to be chained.
//    
//    CheckAndMutateRowRequest req = CheckAndMutateRowRequest.newBuilder()
//        .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
//        .setRowKey(ByteStringer.wrap(new byte[] { 8 }))
//        .setPredicateFilter(RowFilter.Chain.newBuilder()
//            .addFiltersBuilder().setChain(Chain.newBuilder()
//                .addFilters(RowFilter.newBuilder()
//                    .setFamilyNameRegexFilterBytes(ByteStringer.wrap("cf".getBytes())))
//                .addFilters(RowFilter.newBuilder()
//                    .setColumnQualifierRegexFilter(ByteStringer.wrap("c".getBytes())))
//                // this has to be before the value filter.
//                .addFilters(RowFilter.newBuilder()
//                    .setCellsPerColumnLimitFilter(1))
//                .addFilters(RowFilter.newBuilder()
//                    .setValueRangeFilter(ValueRange.newBuilder()
//                        .setStartValueClosed(ByteStringer.wrap("cas1".getBytes()))
//                        .setEndValueClosed(ByteStringer.wrap("cas1".getBytes())))
//                    )
//                )
//            )
//        
//        // SET to false for NULL values expected but true with a value filter
//        // for non-null cas
//        .addTrueMutations(Mutation.newBuilder()
//            .setSetCell(SetCell.newBuilder()
//                .setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()))
//                .setColumnQualifier(ByteStringer.wrap("c".getBytes()))
//                .setValue(ByteStringer.wrap("cas2".getBytes()))
//                .setTimestampMicros(-1)
//                )
//            )
//        .build();
//    System.out.println(req);
//   CheckAndMutateRowResponse resp = store.executor().checkAndMutateRowAsync(req).get();
//   
//  System.out.println("RESP: " + wasMutationApplied(req, resp));
//  
//  ReadRowsRequest rrr = ReadRowsRequest.newBuilder()
//    .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
//    .setRowsLimit(1)
//    .setRows(RowSet.newBuilder()
//        .addRowKeys(ByteStringer.wrap(new byte[] { 8 })))
//    .setFilter(RowFilter.Chain.newBuilder()
//        .addFiltersBuilder()
//        .setChain(Chain.newBuilder()
//            .addFilters(RowFilter.newBuilder()
//                .setFamilyNameRegexFilterBytes(ByteStringer.wrap("cf".getBytes())))
//            .addFilters(RowFilter.newBuilder()
//                .setCellsPerColumnLimitFilter(1)))
//        )
//    .buildPartial();
//
//System.out.println("GET: " + store.session.createAsyncExecutor().readRowsAsync(rrr).get());
//  }
//  
//  // This works ok. Maybe the appends don't?
//  @Test
//  public void singleValueReturn() throws Exception {
//    Tsdb1xBigtableFactory factory = mock(Tsdb1xBigtableFactory.class);
//    MockTSDB tsdb = new MockTSDB();
//    
//    tsdb.config.register(Tsdb1xBigtableDataStore.PROJECT_ID_KEY, 
//        "calcium-post-108621", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.INSTANCE_ID_KEY, 
//        "opentsdb", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.ZONE_ID_KEY, 
//        "us-central1-c", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.JSON_KEYFILE_KEY, 
//        "/Users/clarsen/Documents/opentsdb/bigtable/quickstart/key.json", false, "UT");
//    when(factory.tsdb()).thenReturn(tsdb);
//    
//    Tsdb1xBigtableDataStore store = new Tsdb1xBigtableDataStore(factory, null, 
//        mock(Schema.class));
//    
////  Mutation m = Mutation.newBuilder()
////    .setSetCell(SetCell.newBuilder()
////        .setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()))
////        .setColumnQualifier(ByteStringer.wrap("c".getBytes()))
////        .setValue(ByteStringer.wrap("1".getBytes()))
////        )
////    .build();
////  MutateRowRequest mmr = MutateRowRequest.newBuilder()
////    .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
////    .setRowKey(ByteStringer.wrap(new byte[] { 5 }))
////    .addMutations(m)
////    .build();
////  System.out.println("MUTATION: " + store.executor.mutateRowAsync(mmr).get());
////  Thread.sleep(1000);
////  
////  m = Mutation.newBuilder()
////    .setSetCell(SetCell.newBuilder()
////        .setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()))
////        .setColumnQualifier(ByteStringer.wrap("c".getBytes()))
////        .setValue(ByteStringer.wrap("2".getBytes()))
////        )
////    .build();
////  mmr = MutateRowRequest.newBuilder()
////      .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
////      .setRowKey(ByteStringer.wrap(new byte[] { 5 }))
////      .addMutations(m)
////      .build();
////    System.out.println("MUTATION: " + store.executor.mutateRowAsync(mmr).get());
////    Thread.sleep(1000);
////    
////    m = Mutation.newBuilder()
////        .setSetCell(SetCell.newBuilder()
////            .setFamilyNameBytes(ByteStringer.wrap("cf".getBytes()))
////            .setColumnQualifier(ByteStringer.wrap("c".getBytes()))
////            .setValue(ByteStringer.wrap("3".getBytes()))
////            )
////        .build();
////      mmr = MutateRowRequest.newBuilder()
////          .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
////          .setRowKey(ByteStringer.wrap(new byte[] { 5 }))
////          .addMutations(m)
////          .build();
////        System.out.println("MUTATION: " + store.executor.mutateRowAsync(mmr).get());
////        Thread.sleep(1000);
//  
//  ReadRowsRequest rrr = ReadRowsRequest.newBuilder()
//    .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/usertable".getBytes()))
//    .setRowsLimit(1)
//    .setRows(RowSet.newBuilder()
//      .addRowKeys(ByteStringer.wrap(new byte[] { 5 })))
//      .setFilter(RowFilter.newBuilder()
//          .setCellsPerColumnLimitFilter(1)
//          .setFamilyNameRegexFilterBytes(ByteStringer.wrap("cf".getBytes()))
//      )
//    .buildPartial();
//    
//  System.out.println("MUTATION: " + store.executor.readRowsAsync(rrr).get());
//  store.session.close();
//  }
//  
//  public static boolean wasMutationApplied(
//      CheckAndMutateRowRequest request,
//      CheckAndMutateRowResponse response) {
//
//    // If we have true mods, we want the predicate to have matched.
//    // If we have false mods, we did not want the predicate to have matched.
//    return (request.getTrueMutationsCount() > 0
//        && response.getPredicateMatched())
//        || (request.getFalseMutationsCount() > 0
//        && !response.getPredicateMatched());
//  }
//
//  @Test
//  public void uidassign() throws Exception {
//    Tsdb1xBigtableFactory factory = mock(Tsdb1xBigtableFactory.class);
//    MockTSDB tsdb = new MockTSDB();
//    
//    tsdb.config.register(Tsdb1xBigtableDataStore.PROJECT_ID_KEY, 
//        "calcium-post-108621", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.INSTANCE_ID_KEY, 
//        "opentsdb", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.ZONE_ID_KEY, 
//        "us-central1-c", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.JSON_KEYFILE_KEY, 
//        "/Users/clarsen/Documents/opentsdb/bigtable/quickstart/key.json", false, "UT");
//    when(factory.tsdb()).thenReturn(tsdb);
//    
//    Schema schema = mock(Schema.class);
//    when(schema.metricWidth()).thenReturn(3);
//    
//    Tsdb1xBigtableDataStore store = new Tsdb1xBigtableDataStore(factory, null, 
//        schema);
//    
//    System.out.println(
//        Arrays.toString(store.uidStore().getOrCreateId(null, 
//            UniqueIdType.METRIC, "cl.test5", mock(TimeSeriesDatumId.class), 
//            null).join().id()));
//  }
//
//  @Test
//  public void readUID() throws Exception {
//    Tsdb1xBigtableFactory factory = mock(Tsdb1xBigtableFactory.class);
//    MockTSDB tsdb = new MockTSDB();
//    
//    tsdb.config.register(Tsdb1xBigtableDataStore.PROJECT_ID_KEY, 
//        "calcium-post-108621", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.INSTANCE_ID_KEY, 
//        "opentsdb", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.ZONE_ID_KEY, 
//        "us-central1-c", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.JSON_KEYFILE_KEY, 
//        "/Users/clarsen/Documents/opentsdb/bigtable/quickstart/key.json", false, "UT");
//    when(factory.tsdb()).thenReturn(tsdb);
//    
//    Tsdb1xBigtableDataStore store = new Tsdb1xBigtableDataStore(factory, null, 
//        mock(Schema.class));
//    
//    
//    ReadRowsRequest rrr = ReadRowsRequest.newBuilder()
//        .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/tsdb-uid".getBytes()))
//        .setRows(RowSet.newBuilder()
//            .addRowRanges(RowRange.newBuilder()
//            .setStartKeyClosed(ByteStringer.wrap("my.metric".getBytes()))
//            .setEndKeyOpen(ByteStringer.wrap("my.metrics".getBytes()))))
//        .setFilter(RowFilter.newBuilder()
//            .setFamilyNameRegexFilterBytes(ByteStringer.wrap("id".getBytes()))
//            )
//        .buildPartial();
//    
////    System.out.println("GET: " + store.session.createAsyncExecutor().readRowsAsync(rrr).get());
//    
////  ReadRowsRequest rrr = ReadRowsRequest.newBuilder()
////  .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/tsdb-uid".getBytes()))
////  .setRows(RowSet.newBuilder()
////      .addRowRanges(RowRange.newBuilder()
////      .setStartKeyClosed(ByteStringer.wrap(new byte[] { 0, 0, 1 }))
////      .setEndKeyOpen(ByteStringer.wrap(new byte[] { 1, 0, 0 }))))
////  .setFilter(RowFilter.newBuilder()
////      .setFamilyNameRegexFilterBytes(ByteStringer.wrap("id".getBytes()))
////      )
////  .buildPartial();
//
//  
//  ResultScanner<FlatRow> scnr = store.session.getDataClient().readFlatRows(rrr);
//  
//  while (true) {
//    FlatRow[] rows = scnr.next(25);
//    if (rows == null || rows.length < 1) {
//      System.out.println("ALL DONE");
//      break;
//    }
//    for (final FlatRow row : rows) {
//      System.out.println("  Row: " + Arrays.toString(row.getCells().get(0).getValue().toByteArray()) 
//        + "  " + Bytes.pretty(row.getCells().get(0).getValue().toByteArray(), true)
//        + "  " + new String(row.getRowKey().toByteArray()));
//    }
//  }
//  store.shutdown().join();
////    System.out.println("GET: " + store.session.createAsyncExecutor().readRowsAsync(rrr).get());
//  
//  }
//  
//  @Test
//  public void readTSDB() throws Exception {
//    Tsdb1xBigtableFactory factory = mock(Tsdb1xBigtableFactory.class);
//    MockTSDB tsdb = new MockTSDB();
//    
//    tsdb.config.register(Tsdb1xBigtableDataStore.PROJECT_ID_KEY, 
//        "calcium-post-108621", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.INSTANCE_ID_KEY, 
//        "opentsdb", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.ZONE_ID_KEY, 
//        "us-central1-c", false, "UT");
//    tsdb.config.register(Tsdb1xBigtableDataStore.JSON_KEYFILE_KEY, 
//        "/Users/clarsen/Documents/opentsdb/bigtable/quickstart/key.json", false, "UT");
//    when(factory.tsdb()).thenReturn(tsdb);
//    
//    Tsdb1xBigtableDataStore store = new Tsdb1xBigtableDataStore(factory, null, 
//        mock(Schema.class));
//    
//    
//    ReadRowsRequest rrr = ReadRowsRequest.newBuilder()
//        .setTableNameBytes(ByteString.copyFrom("projects/calcium-post-108621/instances/opentsdb/tables/tsdb".getBytes()))
//        .setRows(RowSet.newBuilder()
//            .addRowRanges(RowRange.newBuilder()
//            .setStartKeyClosed(ByteStringer.wrap(new byte[] { 0, 7, -44 }))
//            .setEndKeyOpen(ByteStringer.wrap((new byte[] { 0, 7, -43 })))))
//        .setFilter(RowFilter.newBuilder()
//            .setFamilyNameRegexFilterBytes(ByteStringer.wrap("t".getBytes()))
//            )
//        .buildPartial();
//
//  ResultScanner<FlatRow> scnr = store.session.getDataClient().readFlatRows(rrr);
//  
//  while (true) {
//    FlatRow[] rows = scnr.next(25);
//    if (rows == null || rows.length < 1) {
//      System.out.println("ALL DONE");
//      break;
//    }
//    for (final FlatRow row : rows) {
//      for (int i = 0; i < row.getCells().size(); i++) {
//        System.out.println("  Row: " + Arrays.toString(row.getRowKey().toByteArray()) 
//          + "  " + Bytes.pretty(row.getRowKey().toByteArray(), true)
//          + "  " + Arrays.toString(row.getCells().get(i).getValue().toByteArray()));
//        }
//    }
//  }
//  
//  System.out.println("TIME: " + Bytes.getInt(new byte[] { 0, 23, 88, -112 }));
//  
//  store.shutdown().join();
////    System.out.println("GET: " + store.session.createAsyncExecutor().readRowsAsync(rrr).get());
//  
//  }
//}

