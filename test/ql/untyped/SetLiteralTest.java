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

import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import net.opentsdb.ql.ast.Node;
import net.opentsdb.ql.ast.untyped.SetLiteral;
import net.opentsdb.ql.ast.untyped.UntypedVisitor;

public class SetLiteralTest {
  @Test
  public void testDefaultConstruction() {
    final SetLiteral set = new SetLiteral.Builder().build();

    assertEquals(set.getChildren().size(), 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testConstruction() {
    final Node<UntypedVisitor> a = mock(Node.class);
    final Node<UntypedVisitor> b = mock(Node.class);
    final Node<UntypedVisitor> c = mock(Node.class);

    final SetLiteral set = new SetLiteral.Builder()
        .add(a).add(b).prepend(c).build();

    assertEquals(set.getChildren().size(), 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadAdd() {
    new SetLiteral.Builder().add(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadPrepend() {
    new SetLiteral.Builder().prepend(null);
  }

  @Test
  public void testShouldParenthesize() {
    final SetLiteral set = mock(SetLiteral.class);
    when(set.shouldParenthesize()).thenCallRealMethod();

    assertEquals(set.shouldParenthesize(), false);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAccept() {
    final Node<UntypedVisitor> a = mock(Node.class);
    final Node<UntypedVisitor> b = mock(Node.class);
    final Node<UntypedVisitor> c = mock(Node.class);
    final Iterator it = mock(Iterator.class);
    when(it.hasNext()).thenReturn(true).thenReturn(true).thenReturn(true)
        .thenReturn(false);
    when(it.next()).thenReturn(a).thenReturn(b).thenReturn(c);

    final List<Node<UntypedVisitor>> list = mock(List.class);
    when(list.iterator()).thenReturn(it);

    final SetLiteral set = mock(SetLiteral.class);
    when(set.getChildren()).thenReturn(list);
    doCallRealMethod().when(set).accept(isA(UntypedVisitor.class));

    final UntypedVisitor visitor = mock(UntypedVisitor.class);
    set.accept(visitor);

    verify(visitor, times(1)).enter(set);
    verify(a, times(1)).accept(visitor);
    verify(b, times(1)).accept(visitor);
    verify(c, times(1)).accept(visitor);
    verify(visitor, times(1)).leave(set);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testEqualsAndHashCode() {
    final Node<UntypedVisitor> a = mock(Node.class);
    final Node<UntypedVisitor> b = mock(Node.class);
    final Node<UntypedVisitor> c = mock(Node.class);

    final SetLiteral set1 = new SetLiteral.Builder()
        .add(a).add(b).build();
    final SetLiteral set2 = new SetLiteral.Builder()
        .add(a).add(b).build();
    final SetLiteral set3 = new SetLiteral.Builder()
        .add(c).build();
    final SetLiteral set4 = new SetLiteral.Builder()
        .add(c).build();

    assertEquals(set1, set2);
    assertEquals(set3, set4);
  }
}

