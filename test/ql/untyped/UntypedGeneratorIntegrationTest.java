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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.junit.Before;
import org.junit.Test;

import net.opentsdb.ql.ast.Node;
import net.opentsdb.ql.ast.untyped.UntypedGenerator;
import net.opentsdb.ql.ast.untyped.UntypedVisitor;

public class UntypedGeneratorIntegrationTest {
  private UntypedGenerator gen;

  @Before
  public void setUp() {
    gen = new UntypedGenerator();
  }

  @Test
  public void testIntegrated() {
    helper("namespace = 'blah' and (application = 'foo' or "
        + "application = 'bar') or not (metric ~ 'baz*') or metric "
        + "in ('klaatu', 'barada', 'nikto')",
        "(((namespace == \"blah\") && ((application == \"foo\") || "
        + "(application == \"bar\"))) || (!(metric ~ \"baz*\"))) || "
        + "(metric in {\"klaatu\", \"barada\", \"nikto\"})");
  }

  @Test
  public void testAssociativity() {
    // Left.
    helper("a and b and c", "(a && b) && c");
    helper("'d' or 'e' or 'f'", "(\"d\" || \"e\") || \"f\"");
  }

  @Test
  public void testPrecedence() {
    // Different.
    helper("a and b in ('c')", "a && (b in {\"c\"})");
    helper("c or b in ('a')", "c || (b in {\"a\"})");

    // Same.
    helper("d and e or f", "(d && e) || f");
    helper("g or h and i", "(g || h) && i");
  }

  @Test
  public void testAnd() {
    helper("1 and 2", "1 && 2");
  }

  @Test
  public void testOr() {
    helper("p or q", "p || q");
  }

  @Test
  public void testIn() {
    helper("host in ('rincewind.dw.com', 'vimes.dwl.com', 7, false)", 
        "host in {\"rincewind.dw.com\", \"vimes.dwl.com\", 7, false}");
  }

  @Test
  public void testEquateString() {
    helper("a = 'str'", "a == \"str\"");
  }

  @Test
  public void testEquateInteger() {
    helper("qps = 4500", "qps == 4500");
  }

  @Test
  public void testLike() {
    helper("colo like 'sp*'", "colo like \"sp*\"");
  }

  @Test
  public void testRegex() {
    helper("host ~ 'prd-yms-01\\.*'", "host ~ \"prd-yms-01\\.*\"");
  }

  @Test
  public void testNot() {
    helper("not true", "!true");
  }

  @Test
  public void testUnnecessaryParentheses() {
    helper("(true and ((false)))", "true && false");
  }

  @Test
  public void testBareIdentifier() {
    helper("retired", "retired");
  }

  @Test
  public void testBoolean() {
    helper("true", "true");
  }

  @Test
  public void testString() {
    helper("'dummy'", "\"dummy\"");
  }

  @Test
  public void testZero() {
    helper("0", "0");
  }

  @Test
  public void testNonzeroInteger() {
    helper("32", "32");
  }

  @Test
  public void testNegativeInteger() {
    helper("-54321", "-54321");
  }

  @Test(expected = ParseCancellationException.class)
  public void testLikeNonString() {
    gen.toAST("colo like false");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testRegexNonString() {
    gen.toAST("host ~ 24");
    fail();
  }

  @Test(expected = NumberFormatException.class)
  public void testIntegerOverflow() {
    gen.toAST("12345678901234567890");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testInvalidIdentifier() {
    gen.toAST("3fail");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testJuxtaposedIdentifiers() {
    gen.toAST("apples oranges");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testBinaryLhsOnly() {
    gen.toAST("fail and");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testBinaryRhsOnly() {
    gen.toAST("or fail");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testUnaryNoOperand() {
    gen.toAST("not");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testUnaryDanglingOperand() {
    gen.toAST("not a b");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testBadParenthesization() {
    gen.toAST("(true and false(");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testMismatchedParentheses() {
    gen.toAST("(true and false))");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testMissingParentheses() {
    gen.toAST("(true and false");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testEmptyParentheses() {
    gen.toAST("()");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testBadOperator() {
    gen.toAST("4 # 17");
    fail();
  }

  @Test(expected = ParseCancellationException.class)
  public void testBadOperand() {
    gen.toAST("16 or %");
    fail();
  }

  /**
   * For test cases that inspect the structure of the generated AST via its
   * stringified representation.
   */
  private void helper(final String actual, final String expected) {
    final Node<UntypedVisitor> root = gen.toAST(actual);

    assertNotNull(root);
    assertEquals(root.toString(), expected);
  }
}

