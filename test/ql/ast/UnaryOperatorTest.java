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
package net.opentsdb.ql.ast;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class UnaryOperatorTest {
  public static class UnOpImpl extends UnaryOperator<Object> {
    public static final String SYMBOL = "^";

    public UnOpImpl(final Node<Object> expr) {
      super(SYMBOL, expr);
    }

    @Override
    public void accept(final Object visitor) {
      // pass
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConstruction() {
    final Node<Object> expr = mock(Node.class);

    final UnOpImpl op = new UnOpImpl(expr);

    assertEquals(UnOpImpl.SYMBOL, op.getSymbol());
    assertEquals(expr, op.getExpr());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEqualsAndHashCode() {
    final Node<Object> expr1 = mock(Node.class);
    final Node<Object> expr2 = mock(Node.class);

    final UnOpImpl op1 = new UnOpImpl(expr1);
    final UnOpImpl op2 = new UnOpImpl(expr1);
    final UnOpImpl op3 = new UnOpImpl(expr2);
    final UnOpImpl op4 = new UnOpImpl(expr2);

    assertEquals(op1, op2);
    assertEquals(op3, op4);
  }

  @Test
  public void testShouldParenthesize() {
    @SuppressWarnings("unchecked")
    final UnaryOperator<Object> op = mock(UnaryOperator.class);
    when(op.shouldParenthesize()).thenCallRealMethod();

    assertEquals(true, op.shouldParenthesize());
  }

  @SuppressWarnings("unchecked")
  private void testToString(final String symbol, 
                            final String exprString, 
                            final boolean shouldParenthesize, 
                            final String expected) {
    final UnaryOperator<Object> op = mock(UnaryOperator.class);
    when(op.getSymbol()).thenReturn(symbol);
    when(op.toString()).thenCallRealMethod();

    final Node<Object> expr = mock(Node.class);
    when(op.getExpr()).thenReturn(expr);
    when(expr.shouldParenthesize()).thenReturn(shouldParenthesize);
    when(expr.toString()).thenReturn(exprString);

    final String result = op.toString();

    verify(op, times(1)).getSymbol();
    verify(op, times(3)).getExpr();
    verify(expr, times(2)).shouldParenthesize();

    assertEquals(result, expected);
  }

  @Test
  public void testWriteTerminalExpression() {
    testToString("%", "x", false, "%x");
  }

  @Test
  public void testWriteNonTerminalExpression() {
    testToString("$", "1 + 2", true, "$(1 + 2)");
  }
}

