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
    
    //String exp = "(a + (b + c)) > b && c > d";
    //String exp = "a + b + c";
    //String exp = "(a + (b + c)) * d";
    //String exp = "(a && b) || (c && true)";
    String exp = "a.foo % b.foo";
    exp = Expression.JEXL_ENGINE.cleanExpression(exp);
    
    Parser parser = new Parser(new StringReader(exp)); //$NON-NLS-1$
    ASTJexlScript script = parser.JexlScript();//parser.parse(new StringReader(exp), null);
    //script.childrenAccept(new MyVisitor(), null);
    JexlNode root = script;
    System.out.println("KIDS: " + root.jjtGetNumChildren());
    System.out.println("KID: " + root.jjtGetChild(0));
    MyVisitor v = new MyVisitor();
    //root.childrenAccept(v, null);
    dumpNode(root, v);
    System.out.println("---------------");
    System.out.println(v.root);
  }
  
  void dumpNode(final JexlNode node, final MyVisitor v) {
//    if (node instanceof ASTIdentifier) {
//      System.out.println("  Ref: " + ((ASTIdentifier) node).getRegister());
//    } else if (node instanceof ASTAdditiveOperator) {
//      
//    }
    System.out.println(node.getClass());
    node.childrenAccept(v, null);    
    
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
  
  class MyVisitor implements ParserVisitor {
    class Binary {
      String op;
      Object left;
      Object right;
      
      public String toString() {
        return "[" + left + "] " + op + " [" + right + "]";
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
    
    Binary root;
    IdAccumulator accumulator = IdAccumulator.NONE;
    Deque<Object> stack = new ArrayDeque<Object>();
    
    @Override
    public Object visit(SimpleNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
      // TODO Auto-generated method stub
      System.out.println("ROOT");
      //node.childrenAccept(this, data);
      return null;
    }

    @Override
    public Object visit(ASTBlock node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTAmbiguous node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTReference node, Object data) {
      if (node.image != null) {
        System.out.println("  Ref: " + node.image);
      }
      return null;
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
      System.out.println("  OR: " + node.image);
      Binary b = new Binary();
      b.op = "||";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
      System.out.println("  AND: " + node.image);
      Binary b = new Binary();
      b.op = "&&";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
      System.out.println("  EQ: " + node.image);
      Binary b = new Binary();
      b.op = "==";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
      System.out.println("  NEQ: " + node.image);
      Binary b = new Binary();
      b.op = "!=";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
      System.out.println("  LT: " + node.image);
      Binary b = new Binary();
      b.op = "<";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
      System.out.println("  GT: " + node.image);
      Binary b = new Binary();
      b.op = ">";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
      System.out.println("  LE: " + node.image);
      Binary b = new Binary();
      b.op = "<=";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
      System.out.println("  GE: " + node.image);
      Binary b = new Binary();
      b.op = ">=";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
     // Object obj = node.childrenAccept(this, data);
      //System.out.println("  Response from node: " + obj);
      return null;
    }

    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
      System.out.println("  AdditiveOperator: " + node.image);
      Binary b = new Binary();
      b.op = node.image;
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
      System.out.println("  MulNode: " + node.image);
      Binary b = new Binary();
      b.op = "*";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
      System.out.println("  DivOp: " + node.image);
      Binary b = new Binary();
      b.op = "/";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
      System.out.println("  ModNode: " + node.image);
      Binary b = new Binary();
      b.op = "%";
      addBinary(b);
      return null;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
      // don't need to do anything here as the numeric literal will
      // check the parent
      return null;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
      System.out.println("  Identifier: " + node.image);

      Object extant = stack.pop();
      if (extant instanceof Binary) {
        if (accumulator == IdAccumulator.LEFT) {
          ((Id) ((Binary) extant).left).id += "." + node.image;
        } else if (accumulator == IdAccumulator.RIGHT) {
          ((Id) ((Binary) extant).right).id += "." + node.image;
        }
        
        Id id = new Id();
        if (node.jjtGetParent() instanceof ASTUnaryMinusNode) {
          id.id = "-" + node.image;
        } else {
          id.id = node.image;
        }
        
        if (((Binary) extant).left == null) {
          ((Binary) extant).left = id;
          accumulator = IdAccumulator.LEFT;
          stack.push(extant);
        } else {
          ((Binary) extant).right = id;
          accumulator = IdAccumulator.RIGHT;
        }
      }
      return null;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
      System.out.println("  True: " + node.image);
      Bool bool = new Bool();
      if (node.jjtGetParent() instanceof ASTNotNode) {
        bool.bool = false;
      } else {
        bool.bool = true;
      }
      
      Object extant = stack.pop();
      if (extant instanceof Binary) {
        if (((Binary) extant).left == null) {
          ((Binary) extant).left = bool;
          stack.push(extant);
        } else {
          ((Binary) extant).right = bool;
        }
      }
      return null;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
      System.out.println("  False: " + node.image);
      Bool bool = new Bool();
      if (node.jjtGetParent() instanceof ASTNotNode) {
        bool.bool = true;
      } else {
        bool.bool = false;
      }
      
      Object extant = stack.pop();
      if (extant instanceof Binary) {
        if (((Binary) extant).left == null) {
          ((Binary) extant).left = bool;
          stack.push(extant);
        } else {
          ((Binary) extant).right = bool;
        }
      }
      return null;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
      System.out.println("  NumericLiteral: " + node.image);      

      NumericLiteral lit = new NumericLiteral();
      lit.number = node.image;
      
      Object extant = stack.pop();
      if (extant instanceof Binary) {
        if (((Binary) extant).left == null) {
          ((Binary) extant).left = lit;
          stack.push(extant);
        } else {
          ((Binary) extant).right = lit;
        }
      }
      return null;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTSizeMethod node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
      System.out.println("   Ref exp: " + node.image);
      return null;
    }
    
    void addBinary(Binary b) {
      accumulator = IdAccumulator.NONE;
      if (root == null) {
        root = b;
      }
      
      if (stack.isEmpty()) {
        stack.push(b);
      } else {
        final Object extant = stack.pop();
        if (extant instanceof Id) {
          b.left = extant;
        } else {
          if (((Binary) extant).left == null) {
            ((Binary) extant).left = b;
            stack.push(extant);
            stack.push(b);
          } else {
            ((Binary) extant).right = b;
            stack.push(b);
          }
        }
      }
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
