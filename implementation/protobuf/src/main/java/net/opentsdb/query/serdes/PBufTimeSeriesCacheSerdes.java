package net.opentsdb.query.serdes;


import com.google.common.collect.Maps;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import net.opentsdb.data.*;


import net.opentsdb.data.pbuf.QueryResultPB;
import net.opentsdb.data.pbuf.TimeSeriesPB;
import net.opentsdb.data.pbuf.TimeStampPB;
import net.opentsdb.data.pbuf.NumericSegmentPB;
import net.opentsdb.data.pbuf.NumericSummarySegmentPB;
import net.opentsdb.data.pbuf.TimeSeriesDataSequencePB;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.pbuf.QueryResultsListPB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.SerdesException;
import net.opentsdb.query.*;
import net.opentsdb.query.cache.QueryCachePlugin.CachedQueryResult;
import net.opentsdb.data.pbuf.TimeSeriesDataPB.TimeSeriesData;

import net.opentsdb.storage.schemas.tsdb1x.NumericCodec;
import net.opentsdb.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * A segmenter that breaks a QueryResult into multiple, smaller QueryResults
 * used to cache the fetched data into blocks.
 *
 */
public class PBufTimeSeriesCacheSerdes implements TimeSeriesCacheSerdes {

    private static final Logger LOG = LoggerFactory.getLogger(PBufTimeSeriesCacheSerdes.class);
//    final PBufSerdesFactory factory = new PBufSerdesFactory();
//    public static final String TYPE = "PBufQuerySegmenter";
//    private final QueryResult result;
//    private final SerdesOptions options;
//    private final QueryNode node;
//    private final QueryContext context;

    // store iterators so you don't have to re-iterate through the same elements
    private List<List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>>> iterators;

    // for each iterator, this stores the borderline TimeSeriesValues that we did not include in the current
    // hour's cache block but must store for the next block, maps iterator type to timeseriesvalue
    private List<HashMap<String, TimeSeriesValue>> borderlineValues;

    // store list of original timeseries id's
    private List<TimeSeriesId> timeseriesIDs;

    public PBufTimeSeriesCacheSerdes() {
      
    }

    public PBufTimeSeriesCacheSerdes(final QueryResult result, final SerdesOptions options) {
        if (result == null) {
            throw new IllegalArgumentException("Query Result to be cached cannot be null.");
        }
        this.result = result;
        this.options = options;
        node = result.source();
        context = node.pipelineContext().queryContext();

        iterators = new ArrayList<>();
        borderlineValues = new ArrayList<>();
        timeseriesIDs = new ArrayList<>();

        for (TimeSeries ts : result.timeSeries()) {
            iterators.add(new ArrayList<>());
            borderlineValues.add(new HashMap<>());
            timeseriesIDs.add(ts.id());
            for (TypedTimeSeriesIterator it : ts.iterators()) {
                iterators.get(iterators.size() - 1).add(it);
            }
        }
    }
    
    public List<QueryResult> segmentResult(final QueryResult result,
                                           final long blocksize) {

        
    }

    @Override
    public byte[] serialize(Collection<QueryResult> results) {

        PBufSerdes serdes = new PBufSerdes(factory, context, new ByteArrayOutputStream());
        QueryResultsListPB.QueryResultsList.Builder convertedResults = QueryResultsListPB.QueryResultsList.newBuilder();
        for (QueryResult result : results) {
            convertedResults.addResults(serdes.serializeResult(result));
        }

        return convertedResults.build().toByteArray();
    }

    @Override
    public byte[][] serialize(final int[] timestamps, 
                              final Collection<QueryResult> results) {
      final byte[][] cache_data = new byte[timestamps.length][];
      final QueryResult[] array_results = new QueryResult[results.size()];
      final Map<Long, TypedTimeSeriesIterator>[] iterators = new Map[results.size()];
      int i = 0;
      for (final QueryResult result : results) {
        array_results[i++] = result;
        Map<Long, TypedTimeSeriesIterator> map = Maps.newHashMap();
        
      }
      
      // calculate start and end bounds of TimeSeriesValues to determine cache blocks
      for (final QueryResult result : results) {

       // for each timeseries, create a new version of the timeseries and versions of iterators,
       // and for each iterator, iterate through and store the valid timeseriesvalues in the new iterators

       // current QueryResultBuilder for this time block
       QueryResultPB.QueryResult.Builder pbufBlockBuilder = QueryResultPB.QueryResult.newBuilder();

       for (List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> it : iterators) {
           // create a TimeSeriesBuilder here, determine what ID to set
           TimeSeriesPB.TimeSeries.Builder currentTsBuilder = TimeSeriesPB.TimeSeries.newBuilder();

           for (TypedTimeSeriesIterator<? extends TimeSeriesDataType> curr : it) {

               TypedTimeSeriesIterator buildIterator = null;
               TimeSeriesData numericData = null;
               if (curr.getType().equals(NumericType.TYPE)) {
                   numericData = convertNumericType(it, curr, threshold, blocksize);
                   buildIterator = new PBufNumericIterator(numericData);
               }
               // test convertNumericSummaryType
               else if (curr.getType().equals(NumericSummaryType.TYPE)) {
                   buildIterator = new PBufNumericSummaryIterator(convertNumericSummaryType(it, curr, threshold, blocksize));
               }

               // add the TypedTimeSeriesIterator to current currentTsBuilder
               if (buildIterator == null) {
                   LOG.debug("Skipping serialization of unkown type: "
                           + curr.getType());
               }
               else {
                   final PBufIteratorSerdes serdes = factory.serdesForType(buildIterator.getType());
                   if (serdes == null) {
                       if (LOG.isDebugEnabled()) {
                           LOG.debug("Skipping serialization of unknown type: "
                                   + buildIterator.getType());
                       }
                       continue;
                   }

                   if (curr.getType().equals(NumericType.TYPE)) {
                       ((PBufNumericTimeSeriesSerdes) serdes).serializeGivenTimes(currentTsBuilder, context, result, buildIterator,
                               numericData.getSegments(0).getStart(), numericData.getSegments(0).getEnd());
//                       serdes.serialize(currentTsBuilder, context, options, result, buildIterator);

                   }
                   else {
                       serdes.serialize(currentTsBuilder, context, result, buildIterator);
                   }
               }
           }

           // set TimeSeries ID, will have to change this
           currentTsBuilder.setId(PBufTimeSeriesId.newBuilder(
                   timeseriesIDs.get(iterators.indexOf(it)))
                   .build()
                   .pbufID());

           // add TimeSeriesBuilder to current pbufBlockBuilder
           pbufBlockBuilder = pbufBlockBuilder.addTimeseries(currentTsBuilder.build());
       }

       // set datasource of PBufQueryResult
       pbufBlockBuilder = pbufBlockBuilder.setDataSource(result.dataSource());

       // TODO: setting timespecification to the default value (from result), will need to modify this
//       TimeSpecification timespec = result.timeSpecification();

//       if (timespec != null) {
//           pbufBlockBuilder.setTimeSpecification(TimeSpecificationPB.TimeSpecification.newBuilder()
//                   .setStart(TimeStampPB.TimeStamp.newBuilder()
//                           .setEpoch(timespec.start().epoch())
//                           .setNanos(timespec.start().nanos())
//                           .setZoneId(timespec.start().timezone().toString())
//                           .build())
//                   .setEnd(TimeStampPB.TimeStamp.newBuilder()
//                           .setEpoch(timespec.end().epoch())
//                           .setNanos(timespec.end().nanos())
//                           .setZoneId(timespec.end().timezone().toString())
//                           .build())
//                   .setTimeZone(timespec.timezone().toString())
//                   .setInterval(timespec.stringInterval()));
//       }

       // convert queryresult to PBufQueryResult and add to total results list
       results.add(new PBufQueryResult(factory, node, pbufBlockBuilder.build()));

       if (resultExhausted()) {
           return results;
       }
   }

   return results;
      return null;
    }
    
    @Override
    public Map<String, CachedQueryResult> deserialize(final byte[] data) {
        Map<String, QueryResult> results = new HashMap<>();
        try {
            QueryResultsListPB.QueryResultsList serializedResults = QueryResultsListPB.QueryResultsList.parseFrom(data);
            for (QueryResultPB.QueryResult res : serializedResults.getResultsList()) {

                results.put(res.getDataSource(), new PBufQueryResult(factory, node, res));
            }
        }
        catch (Exception e) {
            LOG.error("Unexpected exception deserializing data to Query Results");
        }

        return results;
    }


    /**
     * Selects the valid data that fits in the current time block for a NumericType Iterator.
     * @param it Current TimeSeries' list of value iterators.
     * @param curr Specific iterator whose values we are adding to the cache block.
     * @param threshold All values below this threshold will be included in the cache block.
     * @param blocksize Size of the cache block.
     * @return TimeSeriesData used to construct a new TypedTimeSeriesIterator.
     */
    public TimeSeriesData convertNumericType(final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> it,
                                             final TypedTimeSeriesIterator<? extends TimeSeriesDataType> curr,
                                             final long threshold,
                                             final long blocksize) {

        final long span = calculateSpan(blocksize);
        byte encode_on = NumericCodec.encodeOn(span, NumericCodec.LENGTH_MASK);
        // TODO - Avoid this hardcoding
        if (result.resolution().equals(ChronoUnit.MILLIS)) {
            encode_on = 4;
        }
        long previous_offset = -1;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            // store timestamps of first and last values in this cache block
            TimeStampPB.TimeStamp first = null;
            TimeStampPB.TimeStamp last = null;

            final TimeStampPB.TimeStamp.Builder tsBuilder = TimeStampPB.TimeStamp.newBuilder()
                    .setEpoch(threshold - blocksize)
                    .setNanos((threshold - blocksize) * (1000L * 1000L * 1000L));
            if (context.query().getTimezone() != null) {
                tsBuilder.setZoneId(context.query().getTimezone());
            }

            boolean valueSetToBorder = false;
            TimeSeriesValue<NumericType> value = null;
            if (borderlineValues.get(iterators.indexOf(it)).get(curr.getType().toString()) != null) {
                value = (TimeSeriesValue<NumericType>) borderlineValues.get(iterators.indexOf(it)).get(curr.getType().toString());
                valueSetToBorder = true;
            }
            boolean addedLastElement = false;

            while ((value != null && value.timestamp().epoch() < threshold)
                    || (!valueSetToBorder && curr.hasNext() && (value = (TimeSeriesValue<NumericType>) curr.next()) != null
                    && value.timestamp().epoch() < threshold)) {

                if (first == null) {
                    final TimeStampPB.TimeStamp.Builder starting = TimeStampPB.TimeStamp.newBuilder()
                            .setEpoch(value.timestamp().epoch())
                            .setNanos(value.timestamp().nanos());
                    if (value.timestamp().timezone() != null) {
                        starting.setZoneId(value.timestamp().timezone().toString());
                    }
                    first = starting.build();
                }

                // writing value to byte array output stream

                long current_offset = offset(first, value.timestamp(), result.resolution());

                if (current_offset == previous_offset) {
                    throw new SerdesException("With results set to a resolution of "
                            + result.resolution() + " one or more data points with "
                            + "duplicate timestamps would be written at offset: "
                            + current_offset);
                }
                previous_offset = current_offset;
                if (value.value() == null) {
                    // length of 0 + float flag == null value, so nothing following
                    final byte flags = NumericCodec.FLAG_FLOAT;
                    baos.write(Bytes.fromLong(
                            (current_offset << NumericCodec.FLAG_BITS) | flags),
                            8 - encode_on, encode_on);
                } else if (value.value().isInteger()) {
                    final byte[] vle = NumericCodec.vleEncodeLong(
                            value.value().longValue());
                    final byte flags = (byte) (vle.length - 1);
                    final byte[] b = Bytes.fromLong((current_offset << NumericCodec.FLAG_BITS) | flags);
                    baos.write(b, 8 - encode_on, encode_on);
                    baos.write(vle);

                } else {
                    final double v = value.value().doubleValue();
                    final byte[] vle = NumericType.fitsInFloat(v) ?
                            Bytes.fromInt(Float.floatToIntBits((float) v)) :
                            Bytes.fromLong(Double.doubleToLongBits(v));
                    final byte flags = (byte) ((vle.length - 1) | NumericCodec.FLAG_FLOAT);
                    baos.write(Bytes.fromLong(
                            (current_offset << NumericCodec.FLAG_BITS) | flags),
                            8 - encode_on, encode_on);
                    baos.write(vle);
                }

                // nulling borderline value until next cache block
                borderlineValues.get(iterators.indexOf(it)).put(curr.getType().toString(), null);

                // checking whether the iterator is exhausted
                addedLastElement = !curr.hasNext() && value.timestamp().epoch() < threshold;

                final TimeStampPB.TimeStamp.Builder ending = TimeStampPB.TimeStamp.newBuilder()
                        .setEpoch(value.timestamp().epoch())
                        .setNanos(value.timestamp().nanos());
                if (value.timestamp().timezone() != null) {
                    ending.setZoneId(value.timestamp().timezone().toString());
                }
                last = ending.build();
                value = null;
                valueSetToBorder = false;

            }

            // if exhausted values in iterator, nullify borderline value so that next block doesn't include it
            if (addedLastElement) {
                borderlineValues.get(iterators.indexOf(it)).put(curr.getType().getRawType().getName(), null);
            }
            // storing the borderline value for next iteration check
            else {
                borderlineValues.get(iterators.indexOf(it)).put(curr.getType().getRawType().getName(), value);
            }

            final NumericSegmentPB.NumericSegment ns = NumericSegmentPB.NumericSegment.newBuilder()
                    .setEncodedOn(encode_on)
                    .setResolution(result.resolution().ordinal())
                    .setData(ByteString.copyFrom(baos.toByteArray()))
                    .build();

            final long startepoch = first == null ? threshold - blocksize : first.getEpoch();
            final long startnanos = first == null ? (threshold - blocksize) * (1000L * 1000L * 1000L) : first.getNanos();

            final long endepoch = last == null ? threshold : last.getEpoch();
            final long endnanos = last == null ? threshold * (1000L * 1000L * 1000L) : last.getNanos();

            final TimeStampPB.TimeStamp.Builder start = TimeStampPB.TimeStamp.newBuilder()
                    .setEpoch(startepoch)
                    .setNanos(startnanos);

            if (first != null) {
                start.setZoneId(first.getZoneId().toString());
            }
            else if (context.query().getTimezone() != null) {
                start.setZoneId(context.query().getTimezone().toString());
            }

            final TimeStampPB.TimeStamp.Builder end = TimeStampPB.TimeStamp.newBuilder()
                    .setEpoch(endepoch)
                    .setNanos(endnanos);
            if (last != null) {
                end.setZoneId(last.getZoneId().toString());
            }
            else if (context.query().getTimezone() != null) {
                end.setZoneId(context.query().getTimezone().toString());
            }

            return TimeSeriesData.newBuilder()
                    .setType(NumericType.TYPE.getRawType().getName())
                    .addSegments(TimeSeriesDataSequencePB.TimeSeriesDataSegment.newBuilder()
                            .setStart(start)
                            .setEnd(end)
                            .setData(Any.pack(ns)))
                    .build();
        }
        catch (IOException e) {
            throw new SerdesException("Unexpected exception serializing ", e);
        }

    }


    /**
     * Selects the valid data that fits in the current time block for a NumericSummaryType Iterator.
     * @param it Current TimeSeries' list of value iterators.
     * @param curr Specific iterator whose values we are adding to the cache block.
     * @param threshold All values below this threshold will be included in the cache block.
     * @param blocksize Size of the cache block.
     * @return TimeSeriesData used to construct a new TypedTimeSeriesIterator.
     */
    public TimeSeriesData convertNumericSummaryType(final List<TypedTimeSeriesIterator<? extends TimeSeriesDataType>> it,
                                                    final TypedTimeSeriesIterator<? extends TimeSeriesDataType> curr,
                                                    final long threshold,
                                                    final long blocksize) {

        final long span = calculateSpan(blocksize);
        byte encode_on = NumericCodec.encodeOn(span, NumericCodec.LENGTH_MASK);
        // TODO - Avoid this hardcoding
        if (result.resolution().equals(ChronoUnit.MILLIS)) {
            encode_on = 4;
        }
        long previous_offset = -1;
        final Map<Integer, ByteArrayOutputStream> summary_streams = Maps.newHashMap();

        try {
            // store timestamps of first and last values in this cache block
            TimeStampPB.TimeStamp first = null;
            TimeStampPB.TimeStamp last = null;

            final TimeStampPB.TimeStamp.Builder tsBuilder = TimeStampPB.TimeStamp.newBuilder()
                    .setEpoch(threshold - blocksize)
                    .setNanos((threshold - blocksize) * (1000L * 1000L * 1000L));
            if (context.query().getTimezone() != null) {
                tsBuilder.setZoneId(context.query().getTimezone());
            }

            boolean valueSetToBorder = false;
            TimeSeriesValue<NumericSummaryType> value = null;
            if (borderlineValues.get(iterators.indexOf(it)).get(curr.getType().toString()) != null) {
                value = (TimeSeriesValue<NumericSummaryType>) borderlineValues.get(iterators.indexOf(it)).get(curr.getType().toString());
                valueSetToBorder = true;
            }
            boolean addedLastElement = false;

            while ((value != null && value.timestamp().epoch() < threshold)
                    || (!valueSetToBorder && curr.hasNext() && (value = (TimeSeriesValue<NumericSummaryType>) curr.next()) != null
                    && value.timestamp().epoch() < threshold)) {

                if (first == null) {
                    final TimeStampPB.TimeStamp.Builder starting = TimeStampPB.TimeStamp.newBuilder()
                            .setEpoch(value.timestamp().epoch())
                            .setNanos(value.timestamp().nanos());
                    if (value.timestamp().timezone() != null) {
                        starting.setZoneId(value.timestamp().timezone().toString());
                    }
                    first = starting.build();
                }

                // writing value to byte array output stream

                long current_offset = offset(first, value.timestamp(), result.resolution());


                if (current_offset == previous_offset) {
                    throw new SerdesException("With results set to a resolution of "
                            + result.resolution() + " one or more data points with "
                            + "duplicate timestamps would be written at offset: "
                            + current_offset);
                }
                previous_offset = current_offset;



                if (value.value() == null) {
                    // so, if we have already populated our summaries with nulls we
                    // can fill with nulls. But at the start of the iteration we
                    // don't know what to fill with.
                    for (final Map.Entry<Integer, ByteArrayOutputStream> entry :
                            summary_streams.entrySet()) {
                        ByteArrayOutputStream baos = entry.getValue();
                        final byte flags = NumericCodec.FLAG_FLOAT;
                        baos.write(Bytes.fromLong(
                                (current_offset << NumericCodec.FLAG_BITS) | flags),
                                8 - encode_on, encode_on);
                    }
                    continue;
                }

                for (final int summary : value.value().summariesAvailable()) {
                    ByteArrayOutputStream baos = summary_streams.get(summary);
                    if (baos == null) {
                        baos = new ByteArrayOutputStream();
                        summary_streams.put(summary, baos);
                    }

                    NumericType val = value.value().value(summary);
                    if (val == null) {
                        // length of 0 + float flag == null value, so nothing following
                        final byte flags = NumericCodec.FLAG_FLOAT;
                        baos.write(Bytes.fromLong(
                                (current_offset << NumericCodec.FLAG_BITS) | flags),
                                8 - encode_on, encode_on);
                    } else if (val.isInteger()) {
                        final byte[] vle = NumericCodec.vleEncodeLong(val.longValue());
                        final byte flags = (byte) (vle.length - 1);
                        baos.write(Bytes.fromLong(
                                (current_offset << NumericCodec.FLAG_BITS) | flags),
                                8 - encode_on, encode_on);
                        baos.write(vle);
                    } else {
                        final double v = val.doubleValue();
                        final byte[] vle = NumericType.fitsInFloat(v) ?
                                Bytes.fromInt(Float.floatToIntBits((float) v)) :
                                Bytes.fromLong(Double.doubleToLongBits(v));
                        final byte flags = (byte) ((vle.length - 1) | NumericCodec.FLAG_FLOAT);
                        baos.write(Bytes.fromLong(
                                (current_offset << NumericCodec.FLAG_BITS) | flags),
                                8 - encode_on, encode_on);
                        baos.write(vle);
                    }
                }

                // nulling borderline value until next cache block
                borderlineValues.get(iterators.indexOf(it)).put(curr.getType().toString(), null);

                // checking whether the iterator is exhausted
                addedLastElement = !curr.hasNext() && value.timestamp().epoch() < threshold;

                final TimeStampPB.TimeStamp.Builder ending = TimeStampPB.TimeStamp.newBuilder()
                        .setEpoch(value.timestamp().epoch())
                        .setNanos(value.timestamp().nanos());
                if (value.timestamp().timezone() != null) {
                    ending.setZoneId(value.timestamp().timezone().toString());
                }
                last = ending.build();
                value = null;
                valueSetToBorder = false;
            }

            // if exhausted values in iterator, nullify borderline value so that next block doesn't include it
            if (addedLastElement) {
                borderlineValues.get(iterators.indexOf(it)).put(curr.getType().getRawType().getName(), null);
            }
            // storing the borderline value for next iteration check
            else {
                borderlineValues.get(iterators.indexOf(it)).put(curr.getType().getRawType().getName(), value);
            }

            final NumericSummarySegmentPB.NumericSummarySegment.Builder segment_builder =
                    NumericSummarySegmentPB.NumericSummarySegment.newBuilder()
                            .setEncodedOn(encode_on)
                            .setResolution(result.resolution().ordinal());
            for (final Map.Entry<Integer, ByteArrayOutputStream> entry :
                    summary_streams.entrySet()) {
                segment_builder.addData(NumericSummarySegmentPB.NumericSummarySegment.NumericSummary.newBuilder()
                        .setSummaryId(entry.getKey())
                        .setData(ByteString.copyFrom(entry.getValue().toByteArray())));
            }

            final long startepoch = first == null ? threshold - blocksize : first.getEpoch();
            final long startnanos = first == null ? (threshold - blocksize) * (1000L * 1000L * 1000L) : first.getNanos();

            final long endepoch = last == null ? threshold : last.getEpoch();
            final long endnanos = last == null ? threshold * (1000L * 1000L * 1000L) : last.getNanos();

            final TimeStampPB.TimeStamp.Builder start = TimeStampPB.TimeStamp.newBuilder()
                    .setEpoch(startepoch)
                    .setNanos(startnanos);

            if (first != null) {
                start.setZoneId(first.getZoneId().toString());
            }
            else if (context.query().getTimezone() != null) {
                start.setZoneId(context.query().getTimezone().toString());
            }

            final TimeStampPB.TimeStamp.Builder end = TimeStampPB.TimeStamp.newBuilder()
                    .setEpoch(endepoch)
                    .setNanos(endnanos);
            if (last != null) {
                end.setZoneId(last.getZoneId().toString());
            }
            else if (context.query().getTimezone() != null) {
                end.setZoneId(context.query().getTimezone().toString());
            }

            return TimeSeriesData.newBuilder()
                    .setType(NumericSummaryType.TYPE.getRawType().getName())
                    .addSegments(TimeSeriesDataSequencePB.TimeSeriesDataSegment.newBuilder()
                            .setStart(start)
                            .setEnd(end)
                            .setData(Any.pack(segment_builder.build())))
                    .build();
        }
        catch (IOException e) {
            throw new SerdesException("Unexpected exception serializing ", e);
        }

    }


    // TODO - Implement MILLIS
    /**
     * Calculates the offset from the base timestamp at the right resolution.
     * @param base A non-null base time.
     * @param value A non-null value.
     * @param resolution A non-null resolution.
     * @return An offset in the appropriate units.
     */
    private long offset(final TimeStampPB.TimeStamp base,
                        final TimeStamp value,
                        final ChronoUnit resolution) {
        final long seconds;
        switch(resolution) {
            case NANOS:
            case MICROS:
                seconds = value.epoch() - base.getEpoch();
                return (seconds * 1000L * 1000L * 1000L) + (value.nanos() - base.getNanos());
            case MILLIS:
                seconds = value.epoch() - base.getEpoch();
                return (seconds * 1000L);

            default:
                return value.epoch() - base.getEpoch();
        }
    }


    private long calculateSpan(long blocksize) {
        final long span;
        switch(result.resolution()) {
            case NANOS:
            case MICROS:
                span = (blocksize * 1000L * 1000L * 1000L) * 2L;
                break;
            case MILLIS:
                span = blocksize;
                break;
            default:
                span = blocksize;
        }
        return span;
    }

    /**
     * Checks whether all data has been processed and placed
     * into Query Result segments.
     * @return Boolean denoting whether segmentation is complete.
     */
    private boolean resultExhausted() {
        for (HashMap<String, TimeSeriesValue> timeSeries : borderlineValues) {
            for (TimeSeriesValue currentVal : timeSeries.values()) {
                if (currentVal != null) {
                    return false;
                }
            }
        }
        return true;
    }
}
