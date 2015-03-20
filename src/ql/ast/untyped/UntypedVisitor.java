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
 * Implement this interface to visit untyped nodes.
 * This interface provides both an enter and a leave event for non-terminal
 * nodes but only a visit event for terminal nodes.
 *
 * @since 2.3
 */
public interface UntypedVisitor {
  void enter(Add add);
  void leave(Add add);

  void enter(And and);
  void leave(And and);

  void visit(BooleanLiteral bool);

  void enter(Divide divide);
  void leave(Divide divide);

  void enter(Equals equals);
  void leave(Equals equals);

  void enter(In in);
  void leave(In in);

  void visit(IntegerLiteral integer);

  void visit(Key key);
  
  void enter(Like like);
  void leave(Like like);

  void enter(Not not);
  void leave(Not not);

  void enter(Or or);
  void leave(Or or);

  void enter(Regex regex);
  void leave(Regex regex);

  void enter(SetLiteral set);
  void leave(SetLiteral set);

  void visit(StringLiteral string);
}

