// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.ql.ast.untyped;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Objects;

import net.opentsdb.ql.ast.Node;

/**
 * Representation of a set literal in a filter.
 * Owns a list of zero or more children.
 *
 * @since 2.3
 */
public class SetLiteral implements Node<UntypedVisitor> {
  /** All the children of this set. */
  private final List<Node<UntypedVisitor>> children;

  /**
   * Mechanism for creating new, immutable instances of SetLiteral.
   */
  public static class Builder {
    final List<Node<UntypedVisitor>> children;

    public Builder() {
      children = new ArrayList<Node<UntypedVisitor>>();
    }

    /**
     * Append a child to this set literal's child list.
     */
    public Builder add(final Node<UntypedVisitor> child) {
      if (null == child) {
        throw new IllegalArgumentException();
      }

      children.add(child);
      return this;
    }

    /**
     * Prepend a child to this set literal's child list.
     */
    public Builder prepend(final Node<UntypedVisitor> child) {
      if (null == child) {
        throw new IllegalArgumentException();
      }

      children.add(0, child);
      return this;
    }

    /**
     * Instantiate a new set literal with the children so far specified.
     */
    public SetLiteral build() {
        return new SetLiteral(this);
    }
  }

  protected SetLiteral(final Builder builder) {
    this.children = Collections.unmodifiableList(builder.children);
  }

  /**
   * Yields an immutable list of this set literal's children.
   */
  public List<Node<UntypedVisitor>> getChildren() {
    return children;
  }

  @Override
  public boolean shouldParenthesize() {
    // This node has children, but as a unit, they are all terminals and
    // consequently must not be parenthesized.
    return false;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("{");

    if (children.size() > 0) {
      buffer.append(children.get(0).toString());

      for (int i = 1; i < children.size(); ++i) {
        buffer.append(", ")
              .append(children.get(i).toString());
      }
    }

    buffer.append("}");
    return buffer.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(children);
  }

  @Override
  public boolean equals(final Object other) {
    if (null == other) {
      return false;
    }
    if (this == other) {
      return true;
    }
    if (getClass() != other.getClass()) {
      return false;
    }

    final SetLiteral set = (SetLiteral)other;
    return Objects.equal(children, set.children);
  }

  @Override
  public void accept(final UntypedVisitor visitor) {
    visitor.enter(this);

    for (final Node<UntypedVisitor> n : getChildren()) {
      n.accept(visitor);
    }

    visitor.leave(this);
  }
}

