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
package net.opentsdb.ql.untyped;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import net.opentsdb.ql.ast.BinaryOperator;
import net.opentsdb.ql.ast.Node;
import net.opentsdb.ql.ast.untyped.UntypedVisitor;

/**
 * This base class exists to test the binary-operator functionality that
 * would otherwise be boilerplate code copy-pasted among all tests for binary-
 * operator implementations.
 * All tests for binary-operator implementations should subclass this test and
 * provide suitable implementations of its abstract methods.
 */
public abstract class BinaryOperatorContractTestTemplate<
  Operator extends BinaryOperator<UntypedVisitor>> {
  /**
   * Override and yield real instantiation of class.
   */
  public abstract Operator makeOperator(
      final Node<UntypedVisitor> lhs, final Node<UntypedVisitor> rhs);

  /**
   * Override and yield mocked class instantiation.
   */
  public abstract Operator makeMockedOperator();

  @Test
  @SuppressWarnings("unchecked")
  public void testConstruction() {
    final Node<UntypedVisitor> lhs = mock(Node.class);
    final Node<UntypedVisitor> rhs = mock(Node.class);
    final Operator op = makeOperator(lhs, rhs);

    assertEquals(op.getLhs(), lhs);
    assertEquals(op.getRhs(), rhs);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAccept() {
    final Node<UntypedVisitor> lhs = mock(Node.class);
    final Node<UntypedVisitor> rhs = mock(Node.class);
    final Operator op = makeMockedOperator();
    when(op.getLhs()).thenReturn(lhs);
    when(op.getRhs()).thenReturn(rhs);
    doCallRealMethod().when(op).accept(isA(UntypedVisitor.class));

    final UntypedVisitor visitor = mock(UntypedVisitor.class);
    op.accept(visitor);

    verify(op, times(1)).getLhs();
    verify(lhs, times(1)).accept(visitor);
    verify(op, times(1)).getRhs();
    verify(rhs, times(1)).accept(visitor);
  }
}

