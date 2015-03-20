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
 * Abstract representation of a binary operator in any grammar.
 * All binary operators have arity two. They have an identifying symbol and
 * both a left-hand side and a right-hand side. Consequently, they have a
 * canonical string representation.
 * <p>
 * The implementation of {@link Node#accept} is deferred to subclasses.
 *
 * @param Visitor The interface that will visit instances of this operator.
 * @since 2.3
 */
public abstract class BinaryOperator<Visitor> implements Node<Visitor> {
  private final String symbol;
  private final Node<Visitor> lhs;
  private final Node<Visitor> rhs;

  /**
   * Construct a binary operator that will use the given symbol in its
   * stringified form.
   *
   * @param symbol The symbol that defines this operator.
   * @param lhs The left-hand operand.
   * @param rhs The right-hand operand.
   */
  public BinaryOperator(
    final String symbol,
    final Node<Visitor> lhs,
    final Node<Visitor> rhs) {
    if (null == symbol || symbol.isEmpty() || null == lhs ||
      null == rhs) {
      throw new IllegalArgumentException();
    }

    this.symbol = symbol;
    this.lhs = lhs;
    this.rhs = rhs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(symbol, lhs, rhs);
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
    final BinaryOperator op = (BinaryOperator)other;
    return Objects.equal(symbol, op.symbol)
        && Objects.equal(lhs, op.lhs)
        && Objects.equal(rhs, op.rhs);
  }

  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();

    writeExpression(buffer, getLhs());
    buffer.append(" ").append(getSymbol()).append(" ");
    writeExpression(buffer, getRhs());

    return buffer.toString();
  }

  protected void writeExpression(
    final StringBuffer buffer,
    final Node<Visitor> expression) {
    if (expression.shouldParenthesize()) {
      buffer.append("(");
    }

    buffer.append(expression.toString());

    if (expression.shouldParenthesize()) {
      buffer.append(")");
    }
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
   * @return This operator's left-hand operand.
   */
  public Node<Visitor> getLhs() {
    return lhs;
  }

  /**
   * @return This operator's right-hand operand.
   */
  public Node<Visitor> getRhs() {
    return rhs;
  }
}

