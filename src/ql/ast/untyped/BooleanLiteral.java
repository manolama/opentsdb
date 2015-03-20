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

import net.opentsdb.ql.ast.Literal;

/**
 * Representation of a Boolean literal in a filter.
 *
 * @since 2.3
 */
public class BooleanLiteral extends Literal<Boolean, UntypedVisitor> {
  /**
   * Construct a new Boolean-literal instance.
   *
   * @param value The value this literal should contain.
   */
  public BooleanLiteral(final Boolean value) {
    super(value);
  }

  /**
   * Construct a new Boolean-literal instance from an equivalent string.
   *
   * @param boolString String representation of a Boolean value, true or
   * false.
   */
  public BooleanLiteral(final String boolString) {
    this(Boolean.parseBoolean(boolString));
  }

  @Override
  public void accept(final UntypedVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return getValue().toString();
  }
}

