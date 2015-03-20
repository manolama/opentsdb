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
import net.opentsdb.ql.ast.untyped.Key;
import net.opentsdb.ql.ast.untyped.UntypedVisitor;

import org.junit.Test;

public class KeyTest {
  @Test
  public void testConstruction() {
    final String expected = "keykey";
    final Key key = new Key(expected);

    assertEquals(key.getValue(), expected);
  }

  @Test
  public void testAccept() {
    final Key key = mock(Key.class);
    doCallRealMethod().when(key).accept(isA(UntypedVisitor.class));

    final UntypedVisitor visitor = mock(UntypedVisitor.class);
    key.accept(visitor);

    verify(visitor, times(1)).visit(key);
  }
}

