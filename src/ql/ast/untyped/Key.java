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
 * Representation of a key (index or tag) in a filter.
 *
 * @since 2.3
 */
public class Key extends Literal<String, UntypedVisitor> {
  /**
   * Construct a new key (index or tag) instance.
   *
   * @param value The name of the key.
   */
  public Key(final String value) {
    super(value);
  }

  @Override
  public void accept(final UntypedVisitor visitor) {
    visitor.visit(this);
  }
}

