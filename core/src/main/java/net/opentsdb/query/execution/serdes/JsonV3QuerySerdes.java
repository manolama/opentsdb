// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.execution.serdes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp.Op;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.serdes.SerdesOptions;
import net.opentsdb.query.serdes.TimeSeriesSerdes;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Exceptions;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.Pair;

public class JsonV3QuerySerdes implements TimeSeriesSerdes, TSDBPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(
      JsonV3QuerySerdes.class);
  
  @Override
  public String id() {
    return "V3";
  }

  @Override
  public Deferred<Object> initialize(TSDB tsdb) {
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Deferred<Object> serialize(final QueryContext context, 
                                    final SerdesOptions options,
                                    final OutputStream stream, 
                                    final QueryResult result, 
                                    final Span span) {
    if (stream == null) {
      throw new IllegalArgumentException("Output stream may not be null.");
    }
    if (result == null) {
      throw new IllegalArgumentException("Data may not be null.");
    }
    final JsonV2QuerySerdesOptions opts;
    if (options == null) {
      throw new IllegalArgumentException("Options cannot be null.");
    }
     if (!(options instanceof JsonV2QuerySerdesOptions)) {
      throw new IllegalArgumentException("Options were of the wrong type: " 
          + options.getClass());
    }
    opts = (JsonV2QuerySerdesOptions) options;
    
    final JsonGenerator json;
    try {
      json = JSON.getFactory().createGenerator(stream);
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
    
    try {
      json.writeStartObject();
      
    } catch (IOException e) {
      throw new RuntimeException("WTF?", e);
    }
    
    final List<TimeSeries> series;
    final List<Deferred<TimeSeriesStringId>> deferreds;
    if (result.idType() == Const.TS_BYTE_ID) {
      series = Lists.newArrayList(result.timeSeries());
      deferreds = Lists.newArrayListWithCapacity(series.size());
      for (final TimeSeries ts : result.timeSeries()) {
        deferreds.add(((TimeSeriesByteId) ts.id()).decode(false, 
            null /* TODO - implement tracing */));
      }
    } else {
      series = null;
      deferreds = null;
    }
    
    /**
     * Performs the serialization after determining if the serializations
     * need to resolve series IDs.
     */
    class ResolveCB implements Callback<Object, ArrayList<TimeSeriesStringId>> {

      @Override
      public Object call(final ArrayList<TimeSeriesStringId> ids) 
            throws Exception {
        try {
          // serdes time spec if present
          if (result.timeSpecification() != null) {
            json.writeObjectFieldStart("timeSpecification");
            // TODO - ms, second, nanos, etc
            json.writeNumberField("start", result.timeSpecification().start().epoch());
            json.writeNumberField("end", result.timeSpecification().end().epoch());
            json.writeStringField("intervalISO", result.timeSpecification().interval().toString());
            json.writeStringField("interval", result.timeSpecification().stringInterval());
            //json.writeNumberField("intervalNumeric", result.timeSpecification().interval().get(result.timeSpecification().units()));
            if (result.timeSpecification().timezone() != null) {
              json.writeStringField("timeZone", result.timeSpecification().timezone().toString());
            }
            json.writeStringField("units", result.timeSpecification().units().toString());
            json.writeEndObject();
          }
          
          json.writeArrayFieldStart("data");
          int idx = 0;
          
          if (result.timeSeries().size() > 80000000) {
            final List<Pair<Integer, TimeSeries>> pairs = 
                Lists.newArrayListWithExpectedSize(result.timeSeries().size());
            idx = 0;
            for (final TimeSeries ts : result.timeSeries()) {
              pairs.add(new Pair<Integer, TimeSeries>(idx++, ts));
            }
            
            final List<String> sets = 
                Lists.newArrayListWithExpectedSize(result.timeSeries().size());
            pairs.stream().parallel().forEach((pair) -> {
              try {
                ByteArrayOutputStream baos = null;
                JsonGenerator json = null;
                Optional<Iterator<TimeSeriesValue<?>>> optional = 
                    pair.getValue().iterator(NumericType.TYPE);
                if (optional.isPresent()) {
                  baos = new ByteArrayOutputStream();
                  json = JSON.getFactory().createGenerator(baos);
                  writeNumeric(opts, 
                      optional.get(), 
                      json, 
                      ids != null ? ids.get(pair.getKey()) : (TimeSeriesStringId) pair.getValue().id(), 
                      result);
                }
                
                optional = pair.getValue().iterator(NumericArrayType.TYPE);
                if (optional.isPresent()) {
                  baos = new ByteArrayOutputStream();
                  json = JSON.getFactory().createGenerator(baos);
                  writeNumericArray(opts, 
                      optional.get(), 
                      json, 
                      ids != null ? ids.get(pair.getKey()) : (TimeSeriesStringId) pair.getValue().id(), 
                      result);
                } else {
                  return;
                }
                
                
//                final Iterator<TimeSeriesValue<?>> iterator = optional.get();
//                if (!iterator.hasNext()) {
//                  return;
//                }
//                TimeSeriesValue<? extends TimeSeriesDataType> value = 
//                    (TimeSeriesValue<TimeSeriesDataType>) iterator.next();
//                while (value != null && value.timestamp().compare(
//                    Op.LT, opts.start)) {
//                  if (iterator.hasNext()) {
//                    value = (TimeSeriesValue<NumericType>) iterator.next();
//                  } else {
//                    value = null;
//                  }
//                }
//                
//                if (value == null) {
//                  return;
//                }
//                if (value.timestamp().compare(Op.LT, opts.start()) ||
//                    value.timestamp().compare(Op.GT, opts.end)) {
//                  return;
//                }
//                
//                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
//                final JsonGenerator json = JSON.getFactory().createGenerator(baos);
//                json.writeStartObject();
//                

//                
//                json.writeStringField("metric", id.metric());
//                json.writeObjectFieldStart("tags");
//                for (final Entry<String, String> entry : id.tags().entrySet()) {
//                  json.writeStringField(entry.getKey(), entry.getValue());
//                }
//                json.writeEndObject();
//                json.writeArrayFieldStart("aggregateTags");
//                for (final String tag : id.aggregatedTags()) {
//                  json.writeString(tag);
//                }
//                json.writeEndArray();
//                if (result.timeSpecification() == null) {
//                  json.writeObjectFieldStart("NumericType");
//                  
//                  long ts = 0;
//                  while (value != null) {
//                    if (value.timestamp().compare(Op.GT, opts.end())) {
//                      break;
//                    }
//                    
//                    ts = (opts != null && opts.msResolution()) 
//                        ? value.timestamp().msEpoch() 
//                        : value.timestamp().msEpoch() / 1000;
//                    final String ts_string = Long.toString(ts);
//                    if (value.value() == null) {
//                      json.writeNullField(ts_string);
//                    } else if (type == NumericType.TYPE) {
//                      if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
//                        json.writeNumberField(ts_string, 
//                            ((TimeSeriesValue<NumericType>) value).value().longValue());
//                      } else {
//                        json.writeNumberField(ts_string, 
//                            ((TimeSeriesValue<NumericType>) value).value().doubleValue());
//                      }
//                    }
//                    
//                    if (iterator.hasNext()) {
//                      value = (TimeSeriesValue<NumericType>) iterator.next();
//                    } else {
//                      value = null;
//                    }
//                  }
//                  json.writeEndObject();
//                } else {
//                  // just write the values.
//                  json.writeArrayFieldStart("NumericType");
//                  while (value != null) {
//                    if (value.timestamp().compare(Op.GT, opts.end())) {
//                      break;
//                    }
//                    
//                    if (value.value() == null) {
//                      json.writeNull();
//                    } else if (type == NumericType.TYPE) {
//                      if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
//                        json.writeNumber( 
//                            ((TimeSeriesValue<NumericType>) value).value().longValue());
//                      } else {
//                        json.writeNumber(
//                            ((TimeSeriesValue<NumericType>) value).value().doubleValue());
//                      }
//                    }
//                    
//                    if (iterator.hasNext()) {
//                      value = (TimeSeriesValue<NumericType>) iterator.next();
//                    } else {
//                      value = null;
//                    }
//                  }
//                  json.writeEndArray();
//                }
//                
//                json.writeEndObject();
                json.close();
                synchronized(sets) {
                  sets.add(new String(baos.toByteArray(), Const.UTF8_CHARSET));
                }
                baos.close();
              } catch (Exception e) {
                LOG.error("Failed to serialize ts: " + series, e);
                throw new QueryExecutionException("Unexpected exception "
                    + "serializing ts: " + series, 0, e);
              }
            });
            
            idx = 0;
            for (final String set : sets) {
              if (idx++ > 0) {
                json.writeRaw(",");
              }
              json.writeRaw(set);
            }
          } else {
            for (final TimeSeries series : 
              series != null ? series : result.timeSeries()) {
              
              
              
              final TypeToken<? extends TimeSeriesDataType> type;
              Optional<Iterator<TimeSeriesValue<?>>> optional = 
                  series.iterator(NumericType.TYPE);
              System.out.println("  NUM TYPE: " + optional.isPresent());
//              if (optional.isPresent()) {
//                writeNumeric(opts, 
//                    optional.get(), 
//                    json, 
//                    ids != null ? ids.get(idx++) : (TimeSeriesStringId) series.id(), 
//                    result);
//              }
              
              optional = series.iterator(NumericArrayType.TYPE);
              System.out.println("  ARRAY TYPE: " + optional.isPresent());
              if (optional.isPresent()) {
                writeNumericArray(opts, 
                    optional.get(), 
                    json, 
                    ids != null ? ids.get(idx++) : (TimeSeriesStringId) series.id(), 
                    result);
              } else {
                continue;
              }
              
//              final Iterator<TimeSeriesValue<?>> iterator = optional.get();
//              if (!iterator.hasNext()) {
//                continue;
//              }
//              TimeSeriesValue<? extends TimeSeriesDataType> value = 
//                  (TimeSeriesValue<TimeSeriesDataType>) iterator.next();
//              while (value != null && value.timestamp().compare(
//                  Op.LT, opts.start)) {
//                if (iterator.hasNext()) {
//                  value = (TimeSeriesValue<NumericType>) iterator.next();
//                } else {
//                  value = null;
//                }
//              }
//              
//              if (value == null) {
//                continue;
//              }
//              if (value.timestamp().compare(Op.LT, opts.start()) ||
//                  value.timestamp().compare(Op.GT, opts.end)) {
//                continue;
//              }
//              
//              json.writeStartObject();
//              
//              final TimeSeriesStringId id;
//              if (ids != null) {
//                id = (ids.get(idx++));
//              } else {
//                id = (TimeSeriesStringId) series.id();
//              }
//              
//              json.writeStringField("metric", id.metric());
//              json.writeObjectFieldStart("tags");
//              for (final Entry<String, String> entry : id.tags().entrySet()) {
//                json.writeStringField(entry.getKey(), entry.getValue());
//              }
//              json.writeEndObject();
//              json.writeArrayFieldStart("aggregateTags");
//              for (final String tag : id.aggregatedTags()) {
//                json.writeString(tag);
//              }
//              json.writeEndArray();
//              if (result.timeSpecification() == null) {
//                json.writeObjectFieldStart("NumericType");
//                
//                long ts = 0;
//                while (value != null) {
//                  if (value.timestamp().compare(Op.GT, opts.end())) {
//                    break;
//                  }
//                  
//                  ts = (opts != null && opts.msResolution()) 
//                      ? value.timestamp().msEpoch() 
//                      : value.timestamp().msEpoch() / 1000;
//                  final String ts_string = Long.toString(ts);
//                  if (value.value() == null) {
//                    json.writeNullField(ts_string);
//                  } else if (type == NumericType.TYPE) {
//                    if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
//                      json.writeNumberField(ts_string, 
//                          ((TimeSeriesValue<NumericType>) value).value().longValue());
//                    } else {
//                      json.writeNumberField(ts_string, 
//                          ((TimeSeriesValue<NumericType>) value).value().doubleValue());
//                    }
//                  }
//                  
//                  if (iterator.hasNext()) {
//                    value = (TimeSeriesValue<NumericType>) iterator.next();
//                  } else {
//                    value = null;
//                  }
//                }
//                json.writeEndObject();
//              } else {
//                // just write the values.
//                json.writeArrayFieldStart("NumericType");
//                while (value != null) {
//                  if (value.timestamp().compare(Op.GT, opts.end())) {
//                    break;
//                  }
//                  
//                  if (value.value() == null) {
//                    json.writeNull();
//                  } else if (type == NumericType.TYPE) {
//                    if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
//                      json.writeNumber( 
//                          ((TimeSeriesValue<NumericType>) value).value().longValue());
//                    } else {
//                      json.writeNumber(
//                          ((TimeSeriesValue<NumericType>) value).value().doubleValue());
//                    }
//                  }
//                  
//                  if (iterator.hasNext()) {
//                    value = (TimeSeriesValue<NumericType>) iterator.next();
//                  } else {
//                    value = null;
//                  }
//                }
//                json.writeEndArray();
//              }
//              
//              json.writeEndObject();
              
              json.flush();
            }
            // end of the data array
            json.writeEndArray();
            
            // TODO - flag
            //json.writeObjectField("executionGraph", context.query());
            
            // final end of the object.
            json.writeEndObject();
            json.close();
          }
        } catch (Exception e) {
          LOG.error("Unexpected exception", e);
          return Deferred.fromError(new QueryExecutionException(
              "Unexpected exception "
              + "serializing: " + result, 500, e));
        }
        return Deferred.fromResult(null);
      }
      
    }
    
    class ErrorCB implements Callback<Object, Exception> {
      @Override
      public Object call(final Exception ex) throws Exception {
        if (ex instanceof DeferredGroupException) {
          throw (Exception) Exceptions.getCause((DeferredGroupException) ex);
        }
        throw ex;
      }
    }
    
    try {
      if (deferreds != null) {
        return Deferred.group(deferreds)
          .addCallback(new ResolveCB())
          .addErrback(new ErrorCB());
      } else {
        return Deferred.fromResult(new ResolveCB().call(null));
      }
    } catch (InterruptedException e) {
      throw new QueryExecutionException("Failed to resolve IDs", 500, e);
    } catch (Exception e) {
      LOG.error("Unexpected exception", e);
      throw new QueryExecutionException("Failed to resolve IDs", 500, e);
    }
  }

  @Override
  public void deserialize(final SerdesOptions options, 
                          final InputStream stream,
                          final QueryNode node, 
                          final Span span) {
    node.onError(new UnsupportedOperationException("Not implemented for this "
        + "class: " + getClass().getCanonicalName()));
  }

  void writeNumericArray(final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final TimeSeriesStringId id,
      final QueryResult result) throws IOException {
    System.out.println("SERDES ARRAY: " + id);
    json.writeStartObject();
    
    json.writeStringField("metric", id.metric());
    json.writeObjectFieldStart("tags");
    for (final Entry<String, String> entry : id.tags().entrySet()) {
      json.writeStringField(entry.getKey(), entry.getValue());
    }
    json.writeEndObject();
    json.writeArrayFieldStart("aggregateTags");
    for (final String tag : id.aggregatedTags()) {
      json.writeString(tag);
    }
    json.writeEndArray();
    
    json.writeArrayFieldStart("NumericType");
    TimeSeriesValue<NumericArrayType> value = (TimeSeriesValue<NumericArrayType>) iterator.next();
    if (value.value().isInteger()) {
      for (final long v : value.value().longArray()) {
        json.writeNumber(v);
      }
    } else {
      for (final double v : value.value().doubleArray()) {
        json.writeNumber(v);
      }
    }
    json.writeEndArray();
  
    json.writeEndObject();
    json.close();
  }
  
  void writeNumeric(final JsonV2QuerySerdesOptions options, 
      final Iterator<TimeSeriesValue<?>> iterator, 
      final JsonGenerator json,
      final TimeSeriesStringId id,
      final QueryResult result) throws IOException {
    TimeSeriesValue<? extends TimeSeriesDataType> value = 
        (TimeSeriesValue<TimeSeriesDataType>) iterator.next();
    while (value != null && value.timestamp().compare(
        Op.LT, options.start)) {
      if (iterator.hasNext()) {
        value = (TimeSeriesValue<NumericType>) iterator.next();
      } else {
        value = null;
      }
    }
    
    if (value == null) {
      return;
    }
    if (value.timestamp().compare(Op.LT, options.start()) ||
        value.timestamp().compare(Op.GT, options.end)) {
      return;
    }
    json.writeStartObject();
    
    json.writeStringField("metric", id.metric());
    json.writeObjectFieldStart("tags");
    for (final Entry<String, String> entry : id.tags().entrySet()) {
      json.writeStringField(entry.getKey(), entry.getValue());
    }
    json.writeEndObject();
    json.writeArrayFieldStart("aggregateTags");
    for (final String tag : id.aggregatedTags()) {
      json.writeString(tag);
    }
    json.writeEndArray();
    if (result.timeSpecification() == null) {
      json.writeObjectFieldStart("NumericType");
      
      long ts = 0;
      while (value != null) {
        if (value.timestamp().compare(Op.GT, options.end())) {
          break;
        }
        
        ts = (options != null && options.msResolution()) 
            ? value.timestamp().msEpoch() 
            : value.timestamp().msEpoch() / 1000;
        final String ts_string = Long.toString(ts);
        if (value.value() == null) {
          json.writeNullField(ts_string);
        } else {
          if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
            json.writeNumberField(ts_string, 
                ((TimeSeriesValue<NumericType>) value).value().longValue());
          } else {
            json.writeNumberField(ts_string, 
                ((TimeSeriesValue<NumericType>) value).value().doubleValue());
          }
        }
        
        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericType>) iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndObject();
    } else {
      // just write the values.
      json.writeArrayFieldStart("NumericType");
      while (value != null) {
        if (value.timestamp().compare(Op.GT, options.end())) {
          break;
        }
        
        if (value.value() == null) {
          json.writeNull();
        } else {
          if (((TimeSeriesValue<NumericType>) value).value().isInteger()) {
            json.writeNumber( 
                ((TimeSeriesValue<NumericType>) value).value().longValue());
          } else {
            json.writeNumber(
                ((TimeSeriesValue<NumericType>) value).value().doubleValue());
          }
        }
        
        if (iterator.hasNext()) {
          value = (TimeSeriesValue<NumericType>) iterator.next();
        } else {
          value = null;
        }
      }
      json.writeEndArray();
    }
    
    json.writeEndObject();
    json.close();
  }
}
