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

/**
 * All AST nodes must implement this interface.
 * The goal here is to implement visitor pattern. Compilation will proceed with
 * multiple passes, each generating an intermediate representation (IR) of the
 * expression encoded as an abstract syntax tree (AST). However, regarding the
 * implementation, we are faced with at least the two following options:<ul>
 * <li>write a single IR that, when initialized, has fields for annotations
 * such as type that will initially be nulled; or
 * <li>write a unique IR for the output for each pass.
 * </ul><p>
 * The former case allows for a relatively compact codebase at the cost of
 * documenting the invariants/properties that should apply to the AST after
 * each compilation pass. For example, after a certain pass, some nodes may not
 * legally appear in the AST, some annotations must have been generated during
 * that pass, and other annotations will no longer be needed.
 * <p>
 * The latter case allows for a clear, direct representation via the code
 * itself of the properties of the AST after each pass. Illegal nodes will
 * simply be missing, new nodes and new annotations will be present, an
 * unnecessary annotations will have been removed. This further encourages
 * isolation of the passes and limits the effect of any changes made to a
 * particular IR.
 * <p>
 * Consequently, each AST will need its own visitor. We reflect this fact by
 * requiring that the visitor type be passed into the node interface.
 *
 * @param Visitor The class/interface that defines a visitor for this node.
 * @since 2.3
 */
public interface Node<Visitor> {
  /**
   * Indicates whether this node represents an expression that should be
   * parenthesized during stringification.
   *
   * @return true if this node requires parentheses; otherwise, false.
   */
  boolean shouldParenthesize();

  /**
   * Critical part of visitor pattern: each node must be able to accept an
   * instance of the appropriate visitor.
   * The behavior varies based on the implementation. Any node with at least
   * one child node should instruct each child to accept the visitor. All
   * nodes should notify the visitor that it has been visited. In the case of
   * n-ary operators with arity greater than one, the visitor may wish to
   * visit the node before its first operand and after its last operand.
   *
   * @param visitor The visitor instance that this node should accept.
   */
  void accept(Visitor visitor);
}

