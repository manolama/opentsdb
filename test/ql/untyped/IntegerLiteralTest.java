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
import net.opentsdb.ql.ast.untyped.IntegerLiteral;
import net.opentsdb.ql.ast.untyped.UntypedVisitor;

import org.junit.Test;

public class IntegerLiteralTest {
  @Test
  public void testConstruction() {
    final Long expected = Long.valueOf(314159);

    IntegerLiteral integer = new IntegerLiteral(expected);
    assertEquals(integer.getValue(), expected);
  }

  @Test
  public void testAccept() {
    final IntegerLiteral integer = mock(IntegerLiteral.class);
    doCallRealMethod().when(integer).accept(isA(UntypedVisitor.class));

    final UntypedVisitor visitor = mock(UntypedVisitor.class);
    integer.accept(visitor);

    verify(visitor, times(1)).visit(integer);
  }

  @Test
  public void testToString() {
    final IntegerLiteral integer = mock(IntegerLiteral.class);
    when(integer.toString()).thenCallRealMethod();

    final Long value = Long.valueOf(271828);
    when(integer.getValue()).thenReturn(value);

    final String result = integer.toString();

    verify(integer, times(1)).getValue();

    assertEquals(result, value.toString());
  }
}

