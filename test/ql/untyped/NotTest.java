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

import net.opentsdb.ql.ast.Node;
import net.opentsdb.ql.ast.untyped.Not;
import net.opentsdb.ql.ast.untyped.UntypedVisitor;

/**
 * Confirm that the implementation of the logical-not operator correctly
 * supports construction and visitor-acceptance.
 */
public class NotTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testConstruction() {
    final Node<UntypedVisitor> expr = mock(Node.class);
    final Not not = new Not(expr);

    assertEquals(not.getSymbol(), Not.SYMBOL);
    assertEquals(not.getExpr(), expr);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAccept() {
    final Not not = mock(Not.class);
    doCallRealMethod().when(not).accept(isA(UntypedVisitor.class));

    final Node<UntypedVisitor> expr = mock(Node.class);
    when(not.getExpr()).thenReturn(expr);

    final UntypedVisitor visitor = mock(UntypedVisitor.class);
    not.accept(visitor);

    verify(visitor, times(1)).enter(not);
    verify(not, times(1)).getExpr();
    verify(expr, times(1)).accept(visitor);
    verify(visitor, times(1)).leave(not);
  }
}

