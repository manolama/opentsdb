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

import static org.mockito.Mockito.mock;
import net.opentsdb.ql.ast.Node;
import net.opentsdb.ql.ast.untyped.Like;
import net.opentsdb.ql.ast.untyped.UntypedVisitor;

/**
 * Confirm that the implementation of this operator cleaves to {@link
 * BinaryOperator}.
 */
public class LikeTest extends BinaryOperatorContractTestTemplate<Like> {
  @Override
  public Like makeOperator(
    final Node<UntypedVisitor> lhs, final Node<UntypedVisitor> rhs) {
    return new Like(lhs, rhs);
  }

  @Override
  public Like makeMockedOperator() {
    return mock(Like.class);
  }
}

