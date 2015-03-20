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
 * Representation of a string literal in a filter.
 *
 * @since 2.3
 */
public class StringLiteral extends Literal<String, UntypedVisitor> {
  /**
   * Create a new string-literal instance.
   *
   * @param value The string this literal should contain.
   */
  public StringLiteral(final String value) {
    super(value);
  }

  @Override
  public void accept(final UntypedVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return "\"" + getValue() + "\"";
  }
}

