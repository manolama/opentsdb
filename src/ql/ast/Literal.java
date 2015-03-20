// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.ql.ast;

import com.google.common.base.Objects;

/**
 * Abstract representation of a literal in any grammar.
 * The implementation of {@link Node#accept} is deferred to subclasses.
 *
 * @param T The type of data this literal will represent.
 * @param Visitor The interface that will visit instances of this literal.
 * @since 2.3
 */
public abstract class Literal<T, Visitor> implements Node<Visitor> {
  private final T value;

  /**
   * Construct a literal that represents the given value.
   *
   * @param value The value that this literal represents.
   */
  public Literal(final T value) {
    if (null == value) {
      throw new IllegalArgumentException();
    }

    this.value = value;
  }

  /**
   * @return The value that this literal represents.
   */
  public T getValue() {
    return value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object other) {
    if (null == other) {
      return false;
    }
    if (this == other) {
      return true;
    }
    if (getClass() != other.getClass()) {
      return false;
    }

    final Literal<T, Visitor> literal = (Literal<T, Visitor>)other;
    return Objects.equal(value, literal.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  @Override
  public boolean shouldParenthesize() {
    return false;
  }

  @Override
  public String toString() {
    return getValue().toString();
  }
}

