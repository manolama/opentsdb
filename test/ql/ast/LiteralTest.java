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

public class LiteralTest {
  public static class LitImpl extends Literal<String, Object> {
    public LitImpl(final String string) {
      super(string);
    }

    @Override
    public void accept(final Object visitor) {
      // pass
    }
  }

  @Test
  public void testConstruction() {
    final LitImpl lit = new LitImpl("foo");
    assertEquals("foo", lit.getValue());
  }

  @Test
  public void testEqualsAndHashCode() {
    final LitImpl lit1 = new LitImpl("bar");
    final LitImpl lit2 = new LitImpl("bar");
    final LitImpl lit3 = new LitImpl("baz");
    final LitImpl lit4 = new LitImpl("baz");

    assertEquals(lit1, lit2);
    assertEquals(lit3, lit4);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testShouldParenthesize() {
    final Literal<String, Object> lit = mock(Literal.class);
    when(lit.shouldParenthesize()).thenCallRealMethod();

    assertEquals(false, lit.shouldParenthesize());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testToString() {
    final Literal<String, Object> lit = mock(Literal.class);
    when(lit.toString()).thenCallRealMethod();
    when(lit.getValue()).thenReturn("klaatu");

    final String result = lit.toString();
    verify(lit, times(1)).getValue();
    assertEquals(result, "klaatu");
  }
}

