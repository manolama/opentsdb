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

import net.opentsdb.ql.ast.BinaryOperator;
import net.opentsdb.ql.ast.Node;

/**
 * Representation of the equality operator in a filter.
 *
 * @since 2.3
 */
public class Equals extends BinaryOperator<UntypedVisitor> {
  public static final String SYMBOL = "==";

  /**
   * Construct a new equality-operator instance.
   *
   * @param lhs Left-hand side expression.
   * @param rhs Right-hand side expression.
   */
  public Equals(final Node<UntypedVisitor> lhs, final Node<UntypedVisitor> rhs) {
    super(SYMBOL, lhs, rhs);
  }

  @Override
  public void accept(final UntypedVisitor visitor) {
    visitor.enter(this);

    getLhs().accept(visitor);
    getRhs().accept(visitor);

    visitor.leave(this);
  }
}

