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
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

public class BinaryOperatorTest {
  public static class BinOpImpl extends BinaryOperator<Object> {
    public static final String SYMBOL = "^";

    public BinOpImpl(final Node<Object> lhs, final Node<Object> rhs) {
      super(SYMBOL, lhs ,rhs);
    }

    @Override
    public void accept(final Object visitor) {
      // pass
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConstruction() {
    final Node<Object> lhs = mock(Node.class);
    final Node<Object> rhs = mock(Node.class);

    final BinOpImpl op = new BinOpImpl(lhs, rhs);

    assertEquals(BinOpImpl.SYMBOL, op.getSymbol());
    assertEquals(lhs, op.getLhs());
    assertEquals(rhs, op.getRhs());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEqualsAndHashCode() {
    final Node<Object> lhs1 = mock(Node.class);
    final Node<Object> lhs2 = mock(Node.class);
    final Node<Object> rhs1 = mock(Node.class);
    final Node<Object> rhs2 = mock(Node.class);

    final BinOpImpl op1 = new BinOpImpl(lhs1, rhs1);
    final BinOpImpl op2 = new BinOpImpl(lhs1, rhs1);
    final BinOpImpl op3 = new BinOpImpl(lhs2, rhs2);
    final BinOpImpl op4 = new BinOpImpl(lhs2, rhs2);

    assertEquals(op1, op2);
    assertEquals(op3, op4);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testShouldParenthesize() {
    final BinaryOperator<Object> op = mock(BinaryOperator.class);
    when(op.shouldParenthesize()).thenCallRealMethod();

    assertEquals(true, op.shouldParenthesize());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testToString() {
    final BinaryOperator<Object> op = mock(BinaryOperator.class);
    when(op.toString()).thenCallRealMethod();

    final Node<Object> lhs = mock(Node.class);
    final Node<Object> rhs = mock(Node.class);
    when(op.getLhs()).thenReturn(lhs);
    when(op.getRhs()).thenReturn(rhs);

    op.toString();

    verify(op, times(1)).getLhs();
    verify(op, times(1)).writeExpression(isA(StringBuffer.class), eq(lhs));
    verify(op, times(1)).getSymbol();
    verify(op, times(1)).getRhs();
    verify(op, times(1)).writeExpression(isA(StringBuffer.class), eq(rhs));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testToStringBehavior() {
    final BinaryOperator<Object> op = mock(BinaryOperator.class);
    when(op.toString()).thenCallRealMethod();
    doCallRealMethod().when(op)
      .writeExpression(isA(StringBuffer.class), isA(Node.class));
    when(op.getSymbol()).thenReturn("&");

    final Node<Object> lhs = mock(Node.class);
    final Node<Object> rhs = mock(Node.class);
    when(op.getLhs()).thenReturn(lhs);
    when(lhs.toString()).thenReturn("a");
    when(op.getRhs()).thenReturn(rhs);
    when(rhs.toString()).thenReturn("b");

    assertEquals(op.toString(), "a & b");
  }

  @SuppressWarnings("unchecked")
  public void testWriteExpression(final boolean shouldParenthesize,
    final String expr, final String expected) {
    final StringBuffer buffer = new StringBuffer();

    final BinaryOperator<Object> op = mock(BinaryOperator.class);
    doCallRealMethod().when(op)
      .writeExpression(isA(StringBuffer.class), isA(Node.class));

    final Node<Object> node = mock(Node.class);
    when(node.shouldParenthesize()).thenReturn(shouldParenthesize);
    when(node.toString()).thenReturn(expr);

    op.writeExpression(buffer, node);

    verify(node, times(2)).shouldParenthesize();

    assertEquals(expected, buffer.toString());
  }

  @Test
  public void testWriteTerminalExpression() {
    testWriteExpression(false, "foo", "foo");
  }

  @Test
  public void testWriteNonTerminalExpression() {
    testWriteExpression(true, "bar", "(bar)");
  }
}

