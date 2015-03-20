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

import java.util.Stack;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.TerminalNode;

import net.opentsdb.ql.QueryLexer;
import net.opentsdb.ql.QueryListener;
import net.opentsdb.ql.QueryParser;
import net.opentsdb.ql.ast.Node;
import net.opentsdb.ql.ast.util.LoggingErrorListener;

/**
 * Encapsulates both a visitor and a method that applies instances of that
 * visitor to the ANTLR-generated parse tree in order to produce an untyped
 * abstract syntax tree in the data model.
 *
 * @since 2.3
 */
public class UntypedGenerator
{
  /**
   * Generate an AST from the given string filter.
   *
   * @param filter A stringified Boolean expression cleaving to the YAMAS2
   * filter syntax.
   * @return Root of the resulting untyped AST.
   * @throws NumberFormatException If any integer literal is too large to be
   * represented in 64 bits.
   * @throws ParseCancellationException If any other error occurs during
   * parsing or lexing.
   */
  public Node<UntypedVisitor> toAST(final String filter)
  {
      // Lex and parse the given filter.
      final CharStream charStream = new ANTLRInputStream(filter);
      final QueryLexer lexer = new QueryLexer(charStream);
      final LoggingErrorListener loggingErrorListener = new LoggingErrorListener();
      lexer.removeErrorListeners();
      lexer.addErrorListener(loggingErrorListener);
      
      final TokenStream tokenStream = new CommonTokenStream(lexer);
      final QueryParser parser = new QueryParser(tokenStream);
      parser.setErrorHandler(new BailErrorStrategy());
      parser.removeErrorListeners();
      parser.addErrorListener(loggingErrorListener);
      
      final ParserRuleContext parseTree = parser.filter();

      // Generate the AST from the parse tree using visitor pattern.
      final Visitor visitor = new Visitor();
      final ParseTreeWalker walker = new ParseTreeWalker();
      walker.walk(visitor, parseTree);
      return visitor.getRoot();
  }

  /**
   * Visit a parse tree produced by {@link QueryParser} and generate an AST.
   * Note that nodes will be pushed onto the stack in reverse order: for
   * binary operators, for example, the operands will be pushed in order and
   * popped in reverse. This is why the RHS is popped before the LHS.
   */
  class Visitor implements QueryListener {
    /**
     * The evaluation stack.
     * As evaluation proceeds, nodes are pushed onto or popped from the
     * stack as appropriate for the current event (entering or exiting a
     * particular parse-tree node).
     */
    final Stack<Node<UntypedVisitor>> stack;

    Visitor() {
      stack = new Stack<Node<UntypedVisitor>>();
    }

    /**
     * Enter a parse tree produced by {@link QueryParser#numeric_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void enterNumeric_literal(final QueryParser.Numeric_literalContext ctx) {
      // TODO: consider throwing a more descriptive error
      push(new IntegerLiteral(Long.parseLong(getTerminalText(ctx))));
    }

    /**
     * Exit a parse tree produced by {@link QueryParser#numeric_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void exitNumeric_literal(final QueryParser.Numeric_literalContext ctx) {
      // pass -- handling enter instead
    }

    @Override
    public void enterAndExpr(final QueryParser.AndExprContext ctx) {
      // pass -- handling exit instead
    }

    @Override
    public void exitAndExpr(final QueryParser.AndExprContext ctx) {
      final Node<UntypedVisitor> rhs = pop();
      final Node<UntypedVisitor> lhs = pop();
      push(new And(lhs, rhs));
    }

    @Override
    public void enterAtomExpr(final QueryParser.AtomExprContext ctx) {
      // fall-through for negative_expression rule
    }

    @Override
    public void exitAtomExpr(final QueryParser.AtomExprContext ctx) {
      // fall-through for negative_expression rule
    }

    @Override
    public void enterCmpExpr(final QueryParser.CmpExprContext ctx) {
      // fall-through for set_expression rule
    }

    @Override
    public void exitCmpExpr(final QueryParser.CmpExprContext ctx) {
      // fall-through for set_expression rule
    }

    @Override
    public void enterEqExpr(final QueryParser.EqExprContext ctx) {
      // pass -- handling exit instead
    }

    @Override
    public void exitEqExpr(final QueryParser.EqExprContext ctx) {
      final Node<UntypedVisitor> rhs = pop();
      final Node<UntypedVisitor> lhs = pop();
      push(new Equals(lhs, rhs));
    }

    @Override
    public void enterInExpr(final QueryParser.InExprContext ctx) {
      // pass -- handling exit instead
    }

    @Override
    public void exitInExpr(final QueryParser.InExprContext ctx) {
      final Node<UntypedVisitor> setLiteral = pop();
      final Node<UntypedVisitor> lhs = pop();
      push(new In(lhs, setLiteral));
    }

    @Override
    public void enterLikeExpr(final QueryParser.LikeExprContext ctx) {
      // pass -- handling exit instead
    }

    @Override
    public void exitLikeExpr(final QueryParser.LikeExprContext ctx) {
      final Node<UntypedVisitor> rhs = pop();
      final Node<UntypedVisitor> lhs = pop();
      push(new Like(lhs, rhs));
    }

    @Override
    public void enterNegExpr(final QueryParser.NegExprContext ctx) {
      // fall-through for comparative_expression rule
    }

    @Override
    public void exitNegExpr(final QueryParser.NegExprContext ctx) {
      // fall-through for comparative_expression rule
    }

    @Override
    public void enterNotExpr(final QueryParser.NotExprContext ctx) {
      // pass -- handling exit instead
    }

    @Override
    public void exitNotExpr(final QueryParser.NotExprContext ctx) {
      final Node<UntypedVisitor> expr = pop();
      push(new Not(expr));
    }

    @Override
    public void enterOrExpr(final QueryParser.OrExprContext ctx) {
      // pass -- handling exit instead
    }

    @Override
    public void exitOrExpr(final QueryParser.OrExprContext ctx) {
      final Node<UntypedVisitor> rhs = pop();
      final Node<UntypedVisitor> lhs = pop();
      push(new Or(lhs, rhs));
    }

    @Override
    public void enterRegexExpr(final QueryParser.RegexExprContext ctx) {
      // pass -- handling exit instead
    }

    @Override
    public void exitRegexExpr(final QueryParser.RegexExprContext ctx) {
      final Node<UntypedVisitor> rhs = pop();
      final Node<UntypedVisitor> lhs = pop();
      push(new Regex(lhs, rhs));
    }

    @Override
    public void enterSetExpr(final QueryParser.SetExprContext ctx) {
      // fall-through for junctive_expression rule
    }

    @Override
    public void exitSetExpr(final QueryParser.SetExprContext ctx) {
      // fall-through for junctive_expression rule
    }

    /**
     * Enter a parse tree produced by {@link QueryParser#atomic_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void enterAtomic_literal(final QueryParser.Atomic_literalContext ctx)
    {
    }

    /**
     * Exit a parse tree produced by {@link QueryParser#atomic_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void exitAtomic_literal(final QueryParser.Atomic_literalContext ctx)
    {
    }

    /**
     * Enter a parse tree produced by {@link QueryParser#boolean_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void enterBoolean_literal(final QueryParser.Boolean_literalContext ctx) {
      push(new BooleanLiteral(getTerminalText(ctx)));
    }

    /**
     * Exit a parse tree produced by {@link QueryParser#boolean_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void exitBoolean_literal(final QueryParser.Boolean_literalContext ctx) {
      // pass -- handling enter instead
    }

    /**
     * Enter a parse tree produced by {@link QueryParser#set_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void enterSet_literal(final QueryParser.Set_literalContext ctx) {
      // pass -- handling exit instead
    }

    /**
     * Exit a parse tree produced by {@link QueryParser#set_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void exitSet_literal(final QueryParser.Set_literalContext ctx) {
      // At this point, the stack should have been loaded with all of the
      // set literal's children. Consequently, by asking the set
      // literal's context how many children it has, we can discover how
      // many nodes to pop from the evaluation stack.
      //
      // However, the context tells us how many tokens appear beneath the
      // set literal in the parse tree. For any set literal with N
      // syntactically meaningful children, the total token count should
      // be (2*N + 1). Or, in other words, the number of meaningful
      // children is half the token count, rounded down.
      final int childCount = ctx.getChildCount() / 2;

      // Then all we need to do is pop exactly that many children from
      // the stack and add them to the set-literal builder.
      final SetLiteral.Builder builder = new SetLiteral.Builder();
      for (int cardinality = 0; cardinality < childCount; ++cardinality)
      {
        builder.prepend(pop());
      }

      // Finally, push the completed set literal onto the stack.
      push(builder.build());
    }

    /**
     * Enter a parse tree produced by {@link QueryParser#atomic_expression}.
     * @param ctx the parse tree
     */
    @Override
    public void enterAtomic_expression(final QueryParser.Atomic_expressionContext ctx)
    {
    }

    /**
     * Exit a parse tree produced by {@link QueryParser#atomic_expression}.
     * @param ctx the parse tree
     */
    @Override
    public void exitAtomic_expression(final QueryParser.Atomic_expressionContext ctx)
    {
    }

    /**
     * Enter a parse tree produced by {@link QueryParser#string_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void enterString_literal(final QueryParser.String_literalContext ctx)
    {
      final String string = getTerminalText(ctx);
      push(new StringLiteral(string.substring(1, string.length() - 1)));
    }

    /**
     * Exit a parse tree produced by {@link QueryParser#string_literal}.
     * @param ctx the parse tree
     */
    @Override
    public void exitString_literal(final QueryParser.String_literalContext ctx) {
      // pass -- handling enter instead
    }

    /**
     * Enter a parse tree produced by {@link QueryParser#identifier}.
     * @param ctx the parse tree
     */
    @Override
    public void enterIdentifier(final QueryParser.IdentifierContext ctx) {
      push(new Key(getTerminalText(ctx)));
    }

    /**
     * Exit a parse tree produced by {@link QueryParser#identifier}.
     * @param ctx the parse tree
     */
    @Override
    public void exitIdentifier(final QueryParser.IdentifierContext ctx) {
      // pass -- handling enter instead
    }

    /**
     * Enter a parse tree produced by {@link QueryParser#filter}.
     * @param ctx the parse tree
     */
    @Override
    public void enterFilter(final QueryParser.FilterContext ctx)
    {
    }

    /**
     * Exit a parse tree produced by {@link QueryParser#filter}.
     * @param ctx the parse tree
     */
    @Override
    public void exitFilter(final QueryParser.FilterContext ctx)
    {
    }

    @Override
    public void enterEveryRule(final ParserRuleContext ctx)
    {
    }

    @Override
    public void exitEveryRule(final ParserRuleContext ctx)
    {
    }

    @Override
    public void visitErrorNode(final ErrorNode err)
    {
    }

    @Override
    public void visitTerminal(final TerminalNode term)
    {
    }

    /**
     * Push a single node onto the evaluation stack.
     */
    void push(final Node<UntypedVisitor> node) {
      stack.push(node);
    }

    /**
     * Pop a single node from the evaluation stack.
     */
    Node<UntypedVisitor> pop() {
      return stack.pop();
    }

    /**
     * Call this method after visitation has ended to receive a reference
     * to the root of the resulting AST.
     *
     * @throws RuntimeException If the stack has either zero or more than
     * one object.
     */
    Node<UntypedVisitor> getRoot() {
      // POSTCONDITION ENFORCEMENT:
      //
      // If all went well, then one and only one node should remain on
      // the stack, and that node should be the root of the AST produced
      // by visitation. If this is not the case, then the visitor is not
      // in a valid state, and we must throw an exception.
      if (1 != stack.size())
      {
        throw new RuntimeException();
      }

      // Therefore, we can return the node in the stack as the root.
      return pop();
    }

    /**
     * Must be called on a parent node of a terminal node.
     *
     * @return The textual contents of the given context's first child.
     */
    String getTerminalText(final ParserRuleContext ctx) {
      return ((TerminalNode)ctx.getChild(0)).getSymbol().getText();
    }
  }
}

