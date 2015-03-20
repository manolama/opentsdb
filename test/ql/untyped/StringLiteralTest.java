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

import net.opentsdb.ql.ast.untyped.StringLiteral;
import net.opentsdb.ql.ast.untyped.UntypedVisitor;

public class StringLiteralTest {
  @Test
  public void testConstruction() {
    final String expected = "blah";
    final StringLiteral string = new StringLiteral(expected);

    assertEquals(string.getValue(), expected);
  }

  @Test
  public void testAccept() {
    final StringLiteral string = mock(StringLiteral.class);
    doCallRealMethod().when(string).accept(isA(UntypedVisitor.class));

    final UntypedVisitor visitor = mock(UntypedVisitor.class);
    string.accept(visitor);

    verify(visitor, times(1)).visit(string);
  }

  @Test
  public void testToString() {
    final StringLiteral string = mock(StringLiteral.class);
    when(string.toString()).thenCallRealMethod();

    final String value = "bar";
    when(string.getValue()).thenReturn(value);

    final String result = string.toString();

    verify(string, times(1)).getValue();

    assertEquals(result, "\"" + value + "\"");
  }
}

