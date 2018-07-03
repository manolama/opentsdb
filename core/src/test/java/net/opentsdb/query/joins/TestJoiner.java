// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.joins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.JexlInfo;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.commons.jexl2.parser.Token;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import gnu.trove.map.TLongObjectMap;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MockTimeSeries;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.annotation.MockAnnotationIterator;
import net.opentsdb.data.types.numeric.MockNumericTimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.expressions.parser.MetricExpressionLexer;
import net.opentsdb.expressions.parser.MetricExpressionParser;
import net.opentsdb.expressions.parser.MetricExpressionParser.Addsub_arith_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.AndContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Arith_operands_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.ArithmeticContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Arithmetic_operands_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Divmul_arith_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.LogicalContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.LogicalOperandsContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_expr_and_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_expr_not_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_expr_or_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_operand_and_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_operand_not_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_operand_or_orruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.LogicopContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Main_relational_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.MetricContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Minus_metric_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Mod_arith_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.ModuloContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.NotContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.OrContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Paren_arith_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Paren_logical_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Paren_relational_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.ProgContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.RelationalContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Relational_operands_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.RelationalopContext;
import net.opentsdb.expressions.parser.MetricExpressionVisitor;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Join;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.query.processor.expressions.ExpressionProcessorConfig;
import net.opentsdb.utils.Pair;

public class TestJoiner {
  
  @Test
  public void foo() throws Exception {
//    List<JoinSet> joins = Lists.newArrayList();
//    JoinSet set = new JoinSet();
//    set.type = JoinType.INNER;
//    set.metrics = new Pair<String, String>("a", "b");
//    set.joins = Lists.newArrayList(new Pair<String, String>("host", "host"));
//    joins.add(set);
//    
//    DefaultJoin default_join = new DefaultJoin();
//    default_join.type = JoinType.INNER;
//    default_join.tags = Lists.newArrayList("host");
    
    List<Pair<String, String>> joins = Lists.newArrayList(
        new Pair<String, String>("host", "host"));
    
    JoinConfig config = new JoinConfig(JoinType.INNER, joins);
//    set.type = JoinType.INNER;
    List<TimeSeries> mocks = Lists.newArrayList();
    mocks.add(new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "1")
        .build()));
    mocks.add(new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "2")
        .build()));
//    mocks.add(new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
//        .setMetric("a")
//        .addTags("host", "2")
//        .addTags("owner", "bob")
//        .build()));
    mocks.add(new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "3")
        .build()));
    mocks.add(new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("a")
        .addTags("host", "3")
        .addTags("owner", "sudip")
        .build()));
    // right side
    mocks.add(new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("b")
        .addTags("host", "1")
        .build()));
    mocks.add(new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("b")
        .addTags("host", "2")
        .build()));
    mocks.add(new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("b")
        .addTags("host", "2")
        .addTags("owner", "joe")
        .build()));
    mocks.add(new MockTimeSeries(BaseTimeSeriesStringId.newBuilder()
        .setMetric("b")
        .addTags("host", "4")
        .build()));
    
    QueryResult mock = mock(QueryResult.class);
    when(mock.timeSeries()).thenReturn(mocks);
    
    Joiner joiner = new Joiner(config);
    joiner.join(Lists.newArrayList(mock), "a", "b");
    
    System.out.println("-------------");
    // TODO figure out join order based on the expression if present
    KeyedHashedJoinSet hjs = joiner.join_set;
    if (hjs != null) {
      int i = 0;
      for (final Pair<TimeSeries, TimeSeries> pair : hjs) {
        System.out.println("PAIR: " + 
           (pair.getKey() == null ? "null" : pair.getKey().id().toString()) + 
           ", " + 
           (pair.getValue() == null ? "null" : pair.getValue().id().toString()));
        if (i++ > 20) {
          System.out.println("OOOOPS!");
          return;
        }
      }
      System.out.println("DONE Iterating");
    } else {
//      TLongObjectMap<List<TimeSeries>> left = joiner.default_joins.get("a");
//      TLongObjectMap<List<TimeSeries>> right = joiner.default_joins.get("b");
//      
//      final SimpleHashedJoinSet shjs = new SimpleHashedJoinSet(default_join.type, left, right);
//      int i = 0;
//      for (final Pair<TimeSeries, TimeSeries> pair : shjs) {
//        System.out.println("PAIR: " + 
//           (pair.getKey() == null ? "null" : pair.getKey().id().toString()) + 
//           ", " + 
//           (pair.getValue() == null ? "null" : pair.getValue().id().toString()));
//        if (i++ > 20) {
//          System.out.println("OOOOPS!");
//          return;
//        }
//      }
//      System.out.println("DONE Iterating");
    }
  }
  
  @Test
  public void ooos() throws Exception {
    int v = 1 + (3 + 4) * 2;
    System.out.println(v);
  }
  
  @Test
  public void jexly() throws Exception {
    // TODO - collapse literals like (1 + 2) * a
    
    //String exp = "a + (b + (c.foo + r))";
    //String exp = "-(a * b) / -42 * 2 ";
    //String exp = "(a + (b + c)) > b && c > d";
    //String exp = "a.foo11 + (b + c.w.a.b.2)";
    //String exp = "a - (b + c) - d";
    //String exp = "a.foo.goober.nut + d.bert.up / b.up * c.doi - 2.44";
    //String exp = "(a + (b + c)) * d";
    //String exp = "(a && b) || !(c && true)";
    //String exp = "a / -c";
    String exp = "!(a.'if'.meep % b.bar.moo.p) - a.foo.meep";
    //String exp = "(sys.if.out) * (sys.if.in)";
    exp = Expression.JEXL_ENGINE.cleanExpression(exp);
    
    Parser parser = new Parser(new StringReader(exp)); //$NON-NLS-1$
    ASTJexlScript script = parser.JexlScript();//parser.parse(new StringReader(exp), null);
    //script.childrenAccept(new MyVisitor(), null);
    JexlNode root = script;
    System.out.println("KIDS: " + root.jjtGetNumChildren());
    System.out.println("KID: " + root.jjtGetChild(0));
    MyVisitor2 v = new MyVisitor2();
    Object obj = root.jjtGetChild(0).jjtAccept(v, null);
    //dumpNode(root, v);
    System.out.println("---------------");
    //System.out.println("root: " + System.identityHashCode(v.root));
    System.out.println(obj);
  }
  
  @Test
  public void antlry() throws Exception {
    String exp = "(a + (b + d) + v) + c";
    MetricExpressionLexer lexer = new MetricExpressionLexer(new ANTLRInputStream(exp));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    MetricExpressionParser parser = new MetricExpressionParser(tokens);
    AntlrVisitor v = new AntlrVisitor();
    System.out.println("RESULT: " + parser.prog().accept(v));
    System.out.println("DONE!");
  }
  
  void dumpNode(final JexlNode node, final MyVisitor v) {
//    if (node instanceof ASTIdentifier) {
//      System.out.println("  Ref: " + ((ASTIdentifier) node).getRegister());
//    } else if (node instanceof ASTAdditiveOperator) {
//      
//    }
    System.out.println(node.getClass());
    //node.childrenAccept(v, null);
    
//    if (node instanceof ASTReference) {
//      System.out.println(" RESET IDS here");
////      v.last = null;
//      if (v.accumulator == IdAccumulator.LEFT) {
//        v.accumulator = IdAccumulator.RIGHT;
//      } else {
//        v.accumulator = IdAccumulator.NONE;
//      }
//    }
    
    int c = node.jjtGetNumChildren();
    for (int i = 0; i < c; i++) {
      dumpNode(node.jjtGetChild(i), v);
    }
  }
  
  static enum IdAccumulator {
    NONE,
    LEFT,
    RIGHT
  }
  
  class MyVisitor2 implements ParserVisitor {

    class Binary {
      String op;
      Object left;
      Object right;
      
      public String toString() {
        final StringBuilder buf = new StringBuilder();
        if (left instanceof Binary) {
          buf.append("(")
             .append(left)
             .append(")");
        } else {
          buf.append(left);
        }
        buf.append(" ")
           .append(op)
           .append(" ");
        if (right instanceof Binary) {
          buf.append("(")
             .append(right)
             .append(")");
        } else {
          buf.append(right);
        }
        return buf.toString();
        //return "(" + left + ") " + op + " (" + right + ")";
      }
      
      void setLeft(final Object v) {
        if (left == null) {
          left = v;
        } else if (left instanceof String && v instanceof String) {
          left += "." + (String) v;
        } else {
          throw new RuntimeException("LEFT Already set!");
        }
      }
      
      void setRight(final Object v) {
        if (v == left) {
          System.out.println("   LEFT WAS this node alreay!");
          return;
        }
        if (right == null) {
          right = v;
        } else if (right instanceof String && v instanceof String) {
          right += "." + (String) v;
        } else if (right == this) {
          System.out.println("------------- WTF? Right was this!?!?!?!");
          right = v;
        } else if (right != v) {
          System.out.println("************** Right was: " + right + "   Replace with: " + v);
          right = v;
          //throw new RuntimeException("RIGHT Already set! " + v.getClass());
        }
      }
      
      void rotate(final Object v) {
        if (left == null || left == v) {
          left = v;
        } else if (right == null) {
          right = v;
        } else {
          throw new RuntimeException("Node already full!!!");
        }
      }
    }
    
    class Id {
      String id;
      public String toString() {
        return id;
      }
    }
    
    class NumericLiteral {
      String number;
      public String toString() {
        return number;
      }
    }
    
    class Bool {
      boolean bool;
      public String toString() {
        return Boolean.toString(bool);
      }
    }
    
    class Not {
      Object child;
      public String toString() {
        return "!" + 
            (child instanceof Binary ? ("(" + child + ")") : child.toString());
      }
    }
    
    class Negate {
      Object child;
      public String toString() {
        if (child == null) {
          return "-null";
        }
        return "-" + (child instanceof Binary ? ("(" + child + ")") : child.toString());
      }
    }
    
    @Override
    public Object visit(SimpleNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTBlock node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTAmbiguous node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTReference node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      if (node.jjtGetNumChildren() > 1) {
        final StringBuilder buf = new StringBuilder();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
          if (i > 0) {
            buf.append(".");
          }
          Object obj = node.jjtGetChild(i).jjtAccept(this, data);
          if (obj instanceof NumericLiteral) {
            buf.append(((NumericLiteral) obj).number);
          } else {
            buf.append((String) obj);
          }
        }
        return buf.toString();
      } else {
        return node.jjtGetChild(0).jjtAccept(this, data);
      }
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "||";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "&&";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "==";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "!=";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "<";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = ">";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "<=";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = ">=";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Object left = node.jjtGetChild(0).jjtAccept(this, data);
      System.out.println("left: " + left);
      for (int c = 2, size = node.jjtGetNumChildren(); c < size; c += 2) {
        Object right = node.jjtGetChild(c).jjtAccept(this, data);
        System.out.println("right: " + right);
        try {
            JexlNode op = node.jjtGetChild(c - 1);
            if (op instanceof ASTAdditiveOperator) {
                String which = op.image;
                if ("+".equals(which)) {
                  Binary b = new Binary();
                  b.op = "+";
                  b.left = left;
                  b.right = right;
                  left = b;
                    //left = arithmetic.add(left, right);
                    continue;
                }
                if ("-".equals(which)) {
                  Binary b = new Binary();
                  b.op = "-";
                  b.left = left;
                  b.right = right;
                  left = b;
                    //left = arithmetic.subtract(left, right);
                    continue;
                }
                throw new UnsupportedOperationException("unknown operator " + which);
            }
            throw new IllegalArgumentException("unknown operator " + op);
        } catch (ArithmeticException xrt) {
            //JexlNode xnode = findNullOperand(xrt, node, left, right);
            //throw new JexlException(xnode, "+/- error", xrt);
        }
      }
      
      return left;
    }

    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "*";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "/";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "%";
      b.left = node.jjtGetChild(0).jjtAccept(this, data);
      b.right = node.jjtGetChild(1).jjtAccept(this, data);
      return b;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Negate n = new Negate();
      n.child = node.jjtGetChild(0).jjtAccept(this, data);
      return n;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Not n = new Not();
      n.child = node.jjtGetChild(0).jjtAccept(this, data);
      return n;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return node.image;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Bool b = new Bool();
      b.bool = true;
      return b;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      Bool b = new Bool();
      b.bool = false;
      return b;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      NumericLiteral n = new NumericLiteral();
      n.number = node.image;
      return n;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTSizeMethod node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return node.jjtGetChild(0).jjtAccept(this, data);
    }
    
  }
  
  class MyVisitor implements ParserVisitor {
    class Binary {
      String op;
      Object left;
      Object right;
      
      public String toString() {
        final StringBuilder buf = new StringBuilder();
        if (left instanceof Binary) {
          buf.append("(")
             .append(left)
             .append(")");
        } else {
          buf.append(left);
        }
        buf.append(" ")
           .append(op)
           .append(" ");
        if (right instanceof Binary) {
          buf.append("(")
             .append(right)
             .append(")");
        } else {
          buf.append(right);
        }
        return buf.toString();
        //return "(" + left + ") " + op + " (" + right + ")";
      }
      
      void setLeft(final Object v) {
        if (left == null) {
          left = v;
        } else if (left instanceof String && v instanceof String) {
          left += "." + (String) v;
        } else {
          throw new RuntimeException("LEFT Already set!");
        }
      }
      
      void setRight(final Object v) {
        if (v == left) {
          System.out.println("   LEFT WAS this node alreay!");
          return;
        }
        if (right == null) {
          right = v;
        } else if (right instanceof String && v instanceof String) {
          right += "." + (String) v;
        } else if (right == this) {
          System.out.println("------------- WTF? Right was this!?!?!?!");
          right = v;
        } else if (right != v) {
          System.out.println("************** Right was: " + right + "   Replace with: " + v);
          right = v;
          //throw new RuntimeException("RIGHT Already set! " + v.getClass());
        }
      }
      
      void rotate(final Object v) {
        if (left == null || left == v) {
          left = v;
        } else if (right == null) {
          right = v;
        } else {
          throw new RuntimeException("Node already full!!!");
        }
      }
    }
    
    class Id {
      String id;
      public String toString() {
        return id;
      }
    }
    
    class NumericLiteral {
      String number;
      public String toString() {
        return number;
      }
    }
    
    class Bool {
      boolean bool;
      public String toString() {
        return Boolean.toString(bool);
      }
    }
    
    class Not {
      Object child;
      public String toString() {
        return "!" + 
            (child instanceof Binary ? ("(" + child + ")") : child.toString());
      }
    }
    
    class Negate {
      Object child;
      public String toString() {
        return "-" + (child instanceof Binary ? ("(" + child + ")") : child.toString());
      }
    }
    
    Binary root;
    
    @Override
    public Object visit(SimpleNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      // TODO Auto-generated method stub
      System.out.println("ROOT");
      //node.childrenAccept(this, data);
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTBlock node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTAmbiguous node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTReference node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      if (node.jjtGetNumChildren() > 1) {
        final StringBuilder buf = new StringBuilder();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
          if (i > 0) {
            buf.append(".");
          }
          buf.append((String) node.jjtGetChild(i).jjtAccept(this, data));
        }
        return buf.toString();
      } else {
        return node.jjtGetChild(0).jjtAccept(this, data);
      }
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "||";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "&&";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "==";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "!=";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "<";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = ">";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "<=";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = ">=";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data + "  Kids: " + node.jjtGetNumChildren());
      
      // kids can be one of:
      // Refer -> ID
      // Op
      // Ref -> Function
      // Numeric
      
      Binary b = new Binary();
      return handleBinaryNode(b, node, data);
//      System.out.println("       INIT NODE: " + System.identityHashCode(b));
//      if (data != null && data instanceof Binary) {
//        final Binary up = (Binary) data;
//        if (up.left == null) {
//          up.left = b;
//        } else {
//          up.right = b;
//        }
//      } else if (root == null) {
//        root = b;
//      }
//      
//      for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//        final JexlNode n = node.jjtGetChild(i);
//        System.out.println("    AN Kid: " + n);
//        if (n instanceof ASTAdditiveOperator && b.op != null) {
//          // rotate the tree
//          Binary new_node = new Binary();
//          new_node.left = b;
//          if (root == b) {
//            root = new_node;
//          }
//          b = new_node;
//          System.out.println("       NEW B: " + System.identityHashCode(b));
//        }
//        
//        Object response = n.jjtAccept(this, b);
//        if (response != null) {
//          System.out.println("     response: " + response.getClass());
//          if (response instanceof String) {
//            if (b.left == null) {
//              b.left = response;
//            } else if (b.op == null) {
//              b.left += "." + response;
//            } else if (b.right == null) {
//              b.right = response;
//            } else {
//              b.right += "." + response;
//            }
//          } else if (response instanceof NumericLiteral) {
//            if (b.left == null) {
//              b.left = response;
//            } else {
//              b.right = response;
//            }
//          }
//        }
//      }
//      
//      return b;
    }

    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
      Binary b = (Binary) data;
      b.op = node.image;
      return b;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);

      // kids can be one of:
      // Refer -> ID
      // Op
      // Ref -> Function
      // Numeric
      
      Binary b = new Binary();
      b.op = "*";
      return handleBinaryNode(b, node, data);
//      System.out.println("       INIT NODE: " + System.identityHashCode(b));
//      if (data != null && data instanceof Binary) {
//        final Binary up = (Binary) data;
//        if (up.left == null) {
//          up.left = b;
//        } else {
//          up.right = b;
//        }
//      } else if (root == null) {
//        root = b;
//      }
//      
//      for (int i = 0; i < node.jjtGetNumChildren(); i++) {
//        final JexlNode n = node.jjtGetChild(i);
//        System.out.println("    AN Kid: " + n);
//        if (n instanceof ASTAdditiveOperator && b.op != null) {
//          // rotate the tree
//          Binary new_node = new Binary();
//          new_node.left = b;
//          if (root == b) {
//            root = new_node;
//          }
//          b = new_node;
//          System.out.println("       NEW B: " + System.identityHashCode(b));
//        }
//        
//        Object response = n.jjtAccept(this, b);
//        if (response != null) {
//          System.out.println("     response: " + response.getClass());
//          if (response instanceof String) {
//            if (b.left == null) {
//              b.left = response;
//            } else if (b.op == null) {
//              b.left += "." + response;
//            } else if (b.right == null) {
//              b.right = response;
//            } else {
//              b.right += "." + response;
//            }
//          } else if (response instanceof NumericLiteral) {
//            if (b.left == null) {
//              b.left = response;
//            } else {
//              b.right = response;
//            }
//          }
//        }
//      }
//      
//      return b;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "/";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Binary b = new Binary();
      b.op = "%";
      return handleBinaryNode(b, node, data);
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      if (node.jjtGetNumChildren() > 1) {
        System.out.println(" WTF??? Many children?!?!?!");
      }
      Negate n = new Negate();
      n.child = node.jjtGetChild(0).jjtAccept(this, data);
      return n;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      if (node.jjtGetNumChildren() > 1) {
        System.out.println("WTF?? More than one not child???");
      }
      Not n = new Not();
      n.child = node.jjtGetChild(0).jjtAccept(this, data);
      return n;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
      System.out.println("[" + node + "]  data: " + data + "  " + node.image);
      return node.image;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Bool b = new Bool();
      b.bool = true;
      return b;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      Bool b = new Bool();
      b.bool = false;
      return b;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
      System.out.println("[" + node + "]  data: " + data);
      if (node.jjtGetParent() instanceof ASTReference) {
        return node.image;
      }
      NumericLiteral n = new NumericLiteral();
      n.number = node.image;
      return n;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTSizeMethod node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("[" + node + "]  data: " + data);
      return null;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
      System.out.println("[" + node + "]  data: " + data + " KIDS: " + node.jjtGetChild(0));
      if (node.jjtGetNumChildren() > 1) {
        System.out.println("WTF? MORE THAN ONE!!!");
      }
      return node.jjtGetChild(0).jjtAccept(this, data);
    }
    
    Object handleBinaryNode(Binary b, JexlNode node, Object data) {
      System.out.println("       INIT NODE: " + System.identityHashCode(b));
      if (data != null && data instanceof Binary && data != b) {
        final Binary up = (Binary) data;
        up.rotate(b);
      } else if (root == null) {
        root = b;
      }
      
      for (int i = 0; i < node.jjtGetNumChildren(); i++) {
        final JexlNode n = node.jjtGetChild(i);
        System.out.println("    AN Kid: " + n);
        if (n instanceof ASTAdditiveOperator && b.op != null) {
          // rotate the tree
          Binary new_node = new Binary();
          new_node.left = b;
          if (root == b) {
            root = new_node;
          }
          b = new_node;
          System.out.println("       NEW B: " + System.identityHashCode(b));
        }
        
        Object response = n.jjtAccept(this, b);
        if (response == null) {
          System.out.println(" RESPONSE WSA NULL!!! ****************");
        } else {
          System.out.println("     response: " + response.getClass());
          if (response instanceof String) {
            if (b.left == null || b.op == null) {
              b.setLeft(response);
            } else {
              b.setRight(response);
            }
//            if (b.left == null) {
//              b.left = response;
//            } else if (b.op == null) {
//              b.left += "." + response;
//            } else if (b.right == null) {
//              b.right = response;
//            } else {
//              b.right += "." + response;
//            }
          } else if (response != b) {
            if (b.left == null) {
              b.setLeft(response);
            } else {
              b.setRight(response);
            }
//            if (b.left == null) {
//              b.left = response;
//            } else {
//              b.right = response;
//            }
          }
        }
      }
      
      return b;
    }
  }
  
  class AntlrVisitor implements MetricExpressionVisitor<Object> {

    class Binary {
      String op;
      Object left;
      Object right;
      Binary(String op, Object left, Object right) {
        this.op = op;
        this.left = left;
        this.right = right;
      }
      public String toString() {
        final StringBuilder buf = new StringBuilder();
        if (left instanceof Binary) {
          buf.append("(")
             .append(left)
             .append(")");
        } else {
          buf.append(left);
        }
        buf.append(" ")
           .append(op)
           .append(" ");
        if (right instanceof Binary) {
          buf.append("(")
             .append(right)
             .append(")");
        } else {
          buf.append(right);
        }
        return buf.toString();
        //return "(" + left + ") " + op + " (" + right + ")";
      }
      
      void setLeft(final Object v) {
        if (left == null) {
          left = v;
        } else if (left instanceof String && v instanceof String) {
          left += "." + (String) v;
        } else {
          throw new RuntimeException("LEFT Already set!");
        }
      }
      
      void setRight(final Object v) {
        if (v == left) {
          System.out.println("   LEFT WAS this node alreay!");
          return;
        }
        if (right == null) {
          right = v;
        } else if (right instanceof String && v instanceof String) {
          right += "." + (String) v;
        } else if (right == this) {
          System.out.println("------------- WTF? Right was this!?!?!?!");
          right = v;
        } else if (right != v) {
          System.out.println("************** Right was: " + right + "   Replace with: " + v);
          right = v;
          //throw new RuntimeException("RIGHT Already set! " + v.getClass());
        }
      }
      
      void rotate(final Object v) {
        if (left == null || left == v) {
          left = v;
        } else if (right == null) {
          right = v;
        } else {
          throw new RuntimeException("Node already full!!!");
        }
      }
    }
    
    @Override
    public Object visit(ParseTree arg0) {
      System.out.println(arg0.getClass());
      return null;
    }

    @Override
    public Object visitChildren(RuleNode arg0) {
      // TODO Auto-generated method stub
      System.out.println(arg0.getClass());
      return null;
    }

    @Override
    public Object visitErrorNode(ErrorNode arg0) {
      // TODO Auto-generated method stub
      System.out.println(arg0.getClass());
      return null;
    }

    @Override
    public Object visitTerminal(TerminalNode arg0) {
      // TODO Auto-generated method stub
      System.out.println(arg0.getClass() + " Text: " + arg0.getText());
      return arg0.getText();
    }

    @Override
    public Object visitLogicalOperands(LogicalOperandsContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitLogical_expr_or_rule(Logical_expr_or_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitRelational_operands_rule(
        Relational_operands_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitArithmetic(ArithmeticContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass() + " Kids: " + ctx.getChildCount());
      return ctx.getChild(0).accept(this);
    }

    @Override
    public Object visitMod_arith_rule(Mod_arith_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitMain_relational_rule(Main_relational_ruleContext ctx) {
      System.out.println(ctx.getClass());
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visitNot(NotContext ctx) {
      System.out.println(ctx.getClass());
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visitLogical_operand_and_rule(
        Logical_operand_and_ruleContext ctx) {
      System.out.println(ctx.getClass());
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visitRelationalop(RelationalopContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitAnd(AndContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitLogicop(LogicopContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitMinus_metric_rule(Minus_metric_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitArithmetic_operands_rule(
        Arithmetic_operands_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass() + "  Kids: " + ctx.getChildCount());
      return ctx.getChild(0).accept(this);
    }

    @Override
    public Object visitAddsub_arith_rule(Addsub_arith_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass() + " Kids: " + ctx.getChildCount());
      Object left = ctx.getChild(0).accept(this);
      for (int i = 2; i < ctx.getChildCount(); i += 2) {
        Object right = ctx.getChild(i).accept(this);
        Object op = ctx.getChild(i - 1).accept(this);
        left = new Binary((String) op, left, right);
      }
      return left;
    }

    @Override
    public Object visitParen_logical_rule(Paren_logical_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitArith_operands_rule(Arith_operands_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass() + "  Kids: " + ctx.getChildCount());
      return ctx.getChild(0).accept(this);
    }

    @Override
    public Object visitParen_relational_rule(Paren_relational_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitOr(OrContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitParen_arith_rule(Paren_arith_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass() + "  Kids: " + ctx.getChildCount());
      if (ctx.getChildCount() != 3) {
        throw new RuntimeException("Should always be 3?");
      }
      return ctx.getChild(1).accept(this);
//      for (int i = 0; i < ctx.getChildCount(); i++) {
//        System.out.println("   CHILD: " + ctx.getChild(i).getClass() + " [" + ctx.getChild(i) + "]");
//        //ctx.getChild(i).accept(this);
//      }
//      
//      for (int i = 0; i < ctx.getChildCount(); i++) {
//        //System.out.println("   CHILD: " + ctx.getChild(i).getClass() + " [" + ctx.getChild(i) + "]");
//        ctx.getChild(i).accept(this);
//      }
    }

    @Override
    public Object visitLogical_expr_and_rule(Logical_expr_and_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitDivmul_arith_rule(Divmul_arith_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitProg(ProgContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass() + " Kids: " + ctx.getChildCount());
      return ctx.getChild(0).accept(this);
    }

    @Override
    public Object visitLogical(LogicalContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitLogical_operand_or_orrule(
        Logical_operand_or_orruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitLogical_operand_not_rule(
        Logical_operand_not_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitMetric(MetricContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass() + "  Kids: "+ ctx.getChildCount());
      return ctx.getChild(0).accept(this);
    }

    @Override
    public Object visitRelational(RelationalContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitLogical_expr_not_rule(Logical_expr_not_ruleContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }

    @Override
    public Object visitModulo(ModuloContext ctx) {
      // TODO Auto-generated method stub
      System.out.println(ctx.getClass());
      return null;
    }
    
  }
  
//  @Before
//  public void before() throws Exception {
//    expression_builder = Expression.newBuilder()
//        .setId("e1")
//        .setExpression("a + b");
//    
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION)
//        .setTags(Lists.newArrayList("host", "colo"));
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void joinUnionNullId() throws Exception {
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    final IteratorGroups group = new DefaultIteratorGroups();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(null));
//    joiner.join(group);
//  }
//  
//  @Test
//  public void joinUnionMultiType() throws Exception {
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroups group = new DefaultIteratorGroups();
//    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    //group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web02")
//        .addTags("colo", "lax")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "phx")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    //group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//
//    final Map<String, IteratorGroups> joins = joiner.join(group);
//    
//    assertEquals(3, joins.size());
//    String key = "cololaxhostweb01";
//    IteratorGroups join_group = joins.get(key);
//    assertEquals(3, join_group.flattenedIterators().size());
//    IteratorGroup its = 
//        join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(1, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertTrue(its.iterators(NumericType.TYPE).isEmpty());
//    its = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(2, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//    
//    key = "cololaxhostweb02";
//    join_group = joins.get(key);
//    assertEquals(4, join_group.flattenedIterators().size());
//    its = join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(2, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//    its = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(2, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//    
//    key = "colophxhostweb01";
//    join_group = joins.get(key);
//    assertEquals(3, join_group.flattenedIterators().size());
//    its = join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(2, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//    its = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(1, its.flattenedIterators().size());
//    assertTrue(its.iterators(AnnotationType.TYPE).isEmpty());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//  }
//  
//  @Test
//  public void joinUnionOneSeries() throws Exception {
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroups group = new DefaultIteratorGroups();
//    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    
//    final Map<String, IteratorGroups> joins = joiner.join(group);
//    
//    assertEquals(1, joins.size());
//    String key = "cololaxhostweb01";
//    IteratorGroups join_group = joins.get(key);
//    assertEquals(1, join_group.flattenedIterators().size());
//    IteratorGroup join_types = 
//        join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertTrue(join_types.iterators(AnnotationType.TYPE).isEmpty());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//    assertNull(join_group.group(new SimpleStringGroupId("b")));
//  }
//  
//  @Test
//  public void joinIntersectionMultiType() throws Exception {
//    join_builder.setOperator(SetOperator.INTERSECTION);
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroups group = new DefaultIteratorGroups();
//    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    //group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web02")
//        .addTags("colo", "lax")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "phx")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    //group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//
//    final Map<String, IteratorGroups> joins = joiner.join(group);
//    
//    assertEquals(3, joins.size());
//    String key = "cololaxhostweb01";
//    IteratorGroups join_group = joins.get(key);
//    assertEquals(2, join_group.flattenedIterators().size());
//    IteratorGroup join_types = 
//        join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
//    assertTrue(join_types.iterators(NumericType.TYPE).isEmpty());
//    join_types = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
//    assertTrue(join_types.iterators(NumericType.TYPE).isEmpty());
//    
//    key = "cololaxhostweb02";
//    join_group = joins.get(key);
//    assertEquals(4, join_group.flattenedIterators().size());
//    join_types = join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(2, join_types.flattenedIterators().size());
//    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//    join_types = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(2, join_types.flattenedIterators().size());
//    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//    
//    key = "colophxhostweb01";
//    join_group = joins.get(key);
//    assertEquals(2, join_group.flattenedIterators().size());
//    join_types = join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertTrue(join_types.iterators(AnnotationType.TYPE).isEmpty());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//    join_types = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertTrue(join_types.iterators(AnnotationType.TYPE).isEmpty());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//  }
//  
//  @Test (expected = UnsupportedOperationException.class)
//  public void joinUnsupportedJoin() throws Exception {
//    join_builder.setOperator(SetOperator.CROSS);
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroups group = new DefaultIteratorGroups();
//    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    joiner.join(group);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void joinKeyNull() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    joiner.joinKey(null);
//  }
//  
//  @Test
//  public void joinKeyJoinTagsInTags() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .addTags("dept", "KingsGuard")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertEquals("cololaxdeptKingsGuardhostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneAgg() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("dept", "KingsGuard")
//        .addAggregatedTag("colo")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertEquals("colodeptKingsGuardhostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneDisjoint() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("dept", "KingsGuard")
//        .addDisjointTag("colo")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertEquals("colodeptKingsGuardhostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneAggOneDisjoint() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addAggregatedTag("colo")
//        .addDisjointTag("dept")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertEquals("colodepthostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneAggOneDisjointNotIncludingAgg() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"))
//      .setIncludeAggTags(false);
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addAggregatedTag("colo")
//        .addDisjointTag("dept")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertNull(joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneAggOneDisjointNotIncludingDisjoint() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"))
//      .setIncludeDisjointTags(false);
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addAggregatedTag("colo")
//        .addDisjointTag("dept")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertNull(joiner.joinKey(id));
//  }
//
//  @Test
//  public void joinKeyFullJoin() throws Exception {
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION);
//    setConfig();
//    Joiner joiner = new Joiner(config);
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addAggregatedTag("colo")
//        .addDisjointTag("owner")
//        .build();
//    
//    assertEquals("hostweb01coloowner", joiner.joinKey(id));
//    
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION)
//        .setIncludeAggTags(false);
//    setConfig();
//    joiner = new Joiner(config);
//    assertEquals("hostweb01owner", joiner.joinKey(id));
//    
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION)
//        .setIncludeAggTags(false)
//        .setIncludeDisjointTags(false);
//    setConfig();
//    joiner = new Joiner(config);
//    assertEquals("hostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyEmpty() throws Exception {
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION);
//    setConfig();
//    Joiner joiner = new Joiner(config);
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("sys.cpu.user")
//        .build();
//    
//    assertEquals("", joiner.joinKey(id));
//  }
//  
//  private void setConfig() {
//    if (join_builder != null) {
//      expression_builder.setJoin(join_builder.build());
//    }
//    
//    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
//        .setExpression(expression_builder.build())
//          .build();
//  }
}
