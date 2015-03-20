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
package net.opentsdb.ql.ast.untyped;

/**
 * This class provides no-op implementations for each untyped visitor event.
 * Extend this class if you only wish to specify actions for a small subset of
 * visitor events.
 *
 * @since 2.3
 */
public abstract class DefaultUntypedVisitor implements UntypedVisitor {
  @Override public void enter(Add add) { }
  @Override public void leave(Add add) { }

  @Override public void enter(And and) { }
  @Override public void leave(And and) { }

  @Override public void visit(BooleanLiteral bool) { }

  @Override public void enter(Divide divide) { }
  @Override public void leave(Divide divide) { }

  @Override public void enter(Equals equals) { }
  @Override public void leave(Equals equals) { }

  @Override public void enter(In in) { }
  @Override public void leave(In in) { }

  @Override public void visit(IntegerLiteral integer) { }

  @Override public void enter(Like like) { }
  @Override public void leave(Like like) { }

  @Override public void enter(Not not) { }
  @Override public void leave(Not not) { }

  @Override public void enter(Or or) { }
  @Override public void leave(Or or) { }

  @Override public void enter(Regex regex) { }
  @Override public void leave(Regex regex) { }

  @Override public void enter(SetLiteral set) { }
  @Override public void leave(SetLiteral set) { }

  @Override public void visit(StringLiteral string) { }
}

