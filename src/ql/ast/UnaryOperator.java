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
 * Abstract representation of a unary operator in any grammar.
 * All unary operators have arity one. They have an identifying symbol and an
 * associated expression. Consequently, they have a canonical string
 * representation.
 * <p>
 * The implementation of {@link Node#accept} is deferred to subclasses.
 *
 * @param Visitor The interface that will visit instances of this operator.
 * @since 2.3
 */
public abstract class UnaryOperator<Visitor> implements Node<Visitor> {
  private final String symbol;
  private final Node<Visitor> expr;

  /**
   * Construct a unary operator that will use the given symbol in its
   * stringified form.
   *
   * @param symbol The symbol that defines this operator.
   * @param expr The expression to which this operator will be applied.
   */
  public UnaryOperator(
    final String symbol,
    final Node<Visitor> expr) {
    if (null == symbol || symbol.isEmpty() || null == expr) {
      throw new IllegalArgumentException();
    }

    this.symbol = symbol;
    this.expr = expr;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(symbol, expr);
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

    @SuppressWarnings("rawtypes")
    final UnaryOperator op = (UnaryOperator)other;
    return Objects.equal(symbol, op.symbol)
        && Objects.equal(expr, op.expr);
  }

  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();

    buffer.append(getSymbol());
    if (getExpr().shouldParenthesize()) {
      buffer.append("(");
    }
    buffer.append(getExpr().toString());
    if (getExpr().shouldParenthesize()) {
      buffer.append(")");
    }

    return buffer.toString();
  }

  @Override
  public boolean shouldParenthesize() {
    return true;
  }

  /**
   * @return This operator's defining symbol.
   */
  public String getSymbol() {
    return symbol;
  }

  /**
   * @return The expression to which this operator has been applied.
   */
  public Node<Visitor> getExpr() {
    return expr;
  }
}
