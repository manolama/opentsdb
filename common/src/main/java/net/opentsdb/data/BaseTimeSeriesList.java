package net.opentsdb.data;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

public abstract class BaseTimeSeriesList implements List<TimeSeries> {

  protected int size;
  
  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean isEmpty() {
    return size < 1;
  }

  @Override
  public boolean contains(final Object o) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public <T> T[] toArray(final T[] a) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean add(final TimeSeries e) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean remove(final Object o) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean addAll(final Collection<? extends TimeSeries> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean addAll(final int index, 
                        final Collection<? extends TimeSeries> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public TimeSeries set(final int index, final TimeSeries element) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public void add(final int index, final TimeSeries element) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public TimeSeries remove(final int index) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public int indexOf(final Object o) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public int lastIndexOf(final Object o) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public ListIterator<TimeSeries> listIterator() {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public ListIterator<TimeSeries> listIterator(final int index) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

  @Override
  public List<TimeSeries> subList(final int fromIndex, final int toIndex) {
    throw new UnsupportedOperationException("Not implemented for time series.");
  }

}
