package net.opentsdb.query.processor.expressions;

import java.io.StringReader;
import java.util.List;

import org.apache.commons.jexl2.DebugInfo;
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
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.JoinConfig;

public class ExpressionNodeBuilder implements ParserVisitor {

  static enum BranchType {
    VARIABLE,
    SUB_EXP,
    LITERAL_NUMERIC,
    LITERAL_STRING,
    LITERAL_BOOL,
    NULL
  }
  
  static enum ExpOp {
    OR,
    AND,
    EQ,
    NE,
    LT,
    GT,
    LE,
    GE,
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    MOD,
  }
  
  static class ExpNodeConfig extends BaseQueryNodeConfig {
    String left;
    BranchType left_type;
    String right;
    BranchType right_type;
    ExpOp op;
    boolean negate;
    boolean not;
    JoinConfig join;
    
    protected ExpNodeConfig(Builder builder) {
      super(builder);
      left = builder.left;
      left_type = builder.left_type;
      right = builder.right;
      right_type = builder.right_type;
      op = builder.op;
      negate = builder.negate;
      not = builder.not;
    }
    
    public void setNegate(boolean negate) {
      this.negate = negate;
    }
    
    public void setNot(boolean not) {
      this.not = not;
    }
    
    public void setJoin(JoinConfig join) {
      this.join = join;
    }
    
    @Override
    public HashCode buildHashCode() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int compareTo(QueryNodeConfig o) {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public boolean equals(Object o) {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public int hashCode() {
      // TODO Auto-generated method stub
      return 0;
    }
    
    static Builder newBuilder() {
      return new Builder();
    }
    
    public String toString() {
      return new StringBuilder()
          .append("{left=")
          .append(left)
          .append(", leftType=")
          .append(left_type)
          .append(", right=")
          .append(right)
          .append(", rightType=")
          .append(right_type)
          .append(", op=")
          .append(op)
          .append(", negate=")
          .append(negate)
          .append(", not=")
          .append(not)
          .append("}")
          .toString();
    }
    
    static class Builder extends BaseQueryNodeConfig.Builder {
      String left;
      BranchType left_type;
      String right;
      BranchType right_type;
      ExpOp op;
      boolean negate;
      boolean not;
      
      public Builder setLeft(final String left) {
        this.left = left;
        return this;
      }
      
      public Builder setLeftType(final BranchType left_type) {
        this.left_type = left_type;
        return this;
      }
      
      public Builder setRight(final String right) {
        this.right = right;
        return this;
      }
      
      public Builder setRightType(final BranchType right_type) {
        this.right_type = right_type;
        return this;
      }
      
      public Builder setExpOp(final ExpOp op) {
        this.op = op;
        return this;
      }
      
      public Builder setNegate(final boolean negate) {
        this.negate = negate;
        return this;
      }
      
      public Builder setNot(final boolean not) {
        this.not = not;
        return this;
      }
      
      @Override
      public QueryNodeConfig build() {
        return new ExpNodeConfig(this);
      }
      
    }
  }
  
  int cntr = 0;
  List<ExpNodeConfig> nodes = Lists.newArrayList();
  String exp_id;
  
  List<ExpNodeConfig> parse(final String exp, String id) {
    exp_id = id;
    
    JexlInfo debug = new DebugInfo(null, 0, 0);
    StringReader rdr = new StringReader(exp);
    Parser parser = new Parser(rdr);
    try {
      ASTJexlScript script = parser.parse(rdr, debug);
      if (script.jjtGetNumChildren() > 1) {
        throw new RuntimeException("WTF? A script with more than one root???");
      }
      Object obj = script.jjtAccept(this, null);
      if (obj instanceof ExpNodeConfig) {
        //nodes.add((ExpNodeConfig) obj);
      } else {
        System.out.println("Root node can't be of type: " + obj.getClass());
      }
    } catch (ParseException e) {
      System.out.println("DEBUG: " + debug.debugString());
      throw new JexlException.Parsing(debug, exp, e);
    }
    
    return nodes;
  }
  
  interface ExpNode {
    
  }
  
  class Identifier implements ExpNode {
    public String id;
    Identifier(final String id) {
      this.id = id;
    }
    public String toString() {
      return id;
    }
  }
  
  class NumericLiteral implements ExpNode {
    String number;
    NumericLiteral(final String number) {
      this.number = number;
    }
    public String toString() {
      return number;
    }
  }
  
  class Bool implements ExpNode {
    boolean bool;
    Bool(final boolean bool) {
      this.bool = bool;
    }
    public String toString() {
      return Boolean.toString(bool);
    }
  }
  
  class Null implements ExpNode {
    public String toString() {
      return null;
    }
  }
  
  public Object newBinary(final ExpOp op, Object left, Object right) {
    // if both sides are numerics then we just add em
    if (left instanceof NumericLiteral && right instanceof NumericLiteral) {
      throw new RuntimeException("TODO!");
    }
    
    // here we can cleanup, e.g. merge numerics
    ExpNodeConfig.Builder builder = ExpNodeConfig.newBuilder()
        .setExpOp(op);
    setBranch(builder, left, true);
    setBranch(builder, right, false);
    // TODO - prepend exp config ID
    builder.setId(exp_id + "_SubExp#" + cntr++);
    final ExpNodeConfig config = (ExpNodeConfig) builder.build();
    nodes.add(config);
    return config;
  }
  
  void setBranch(ExpNodeConfig.Builder builder, Object obj, boolean is_left) {
    if (obj instanceof Null) {
      if (is_left) {
        builder.setLeftType(BranchType.NULL);
      } else {
        builder.setRightType(BranchType.NULL);
      }
    } else if (obj instanceof NumericLiteral) {
      if (is_left) {
        builder.setLeft(((NumericLiteral) obj).number)
               .setLeftType(BranchType.LITERAL_NUMERIC);
      } else {
        builder.setRight(((NumericLiteral) obj).number)
               .setRightType(BranchType.LITERAL_NUMERIC);
      }
    } else if (obj instanceof Bool) {
      if (is_left) {
        builder.setLeft(Boolean.toString(((Bool) obj).bool))
               .setLeftType(BranchType.LITERAL_BOOL);
      } else {
        builder.setRight(Boolean.toString(((Bool) obj).bool))
               .setRightType(BranchType.LITERAL_BOOL);
      }
    } else if (obj instanceof Identifier) {
      if (is_left) {
        builder.setLeft(((Identifier) obj).id)
               .setLeftType(BranchType.VARIABLE);
      } else {
        builder.setRight(((Identifier) obj).id)
               .setRightType(BranchType.VARIABLE);
      }
    } else if (obj instanceof ExpNodeConfig) {
      if (is_left) {
        builder.setLeft(((ExpNodeConfig) obj).getId())
               .setLeftType(BranchType.SUB_EXP);
      } else {
        builder.setRight(((ExpNodeConfig) obj).getId())
               .setRightType(BranchType.SUB_EXP);
      }
    } else {
      throw new RuntimeException("NEED TO HANDLE: " + obj.getClass());
    }
  }
  
  @Override
  public Object visit(SimpleNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTJexlScript node, Object data) {
    if (node.jjtGetNumChildren() > 1) {
      throw new RuntimeException("WTF? A script with more than one root???");
    }
    return node.jjtGetChild(0).jjtAccept(this, data);
  }

  @Override
  public Object visit(ASTBlock node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTAmbiguous node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTIfStatement node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTWhileStatement node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTForeachStatement node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTReturnStatement node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTAssignment node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTVar node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTReference node, Object data) {
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
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTOrNode node, Object data) {
    return newBinary(ExpOp.OR, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTAndNode node, Object data) {
    return newBinary(ExpOp.AND, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTBitwiseOrNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTBitwiseXorNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTBitwiseAndNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTEQNode node, Object data) {
    return newBinary(ExpOp.EQ, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTNENode node, Object data) {
    return newBinary(ExpOp.NE, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTLTNode node, Object data) {
    return newBinary(ExpOp.LT, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTGTNode node, Object data) {
    return newBinary(ExpOp.GT, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTLENode node, Object data) {
    return newBinary(ExpOp.LE, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTGENode node, Object data) {
    return newBinary(ExpOp.GE, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTERNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTNRNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTAdditiveNode node, Object data) {
    Object left = node.jjtGetChild(0).jjtAccept(this, data);
    System.out.println("left: " + left);
    for (int c = 2, size = node.jjtGetNumChildren(); c < size; c += 2) {
      Object right = node.jjtGetChild(c).jjtAccept(this, data);
      System.out.println("right: " + right);
      try {
          JexlNode op = node.jjtGetChild(c - 1);
          System.out.println("  op: " + op.image);
          if (op instanceof ASTAdditiveOperator) {
              String which = op.image;
              if ("+".equals(which)) {
//                Binary b = new Binary();
//                b.op = "+";
//                b.left = left;
//                b.right = right;
//                left = b;
                left = newBinary(ExpOp.ADD, left, right);
                System.out.println("left is now: " + left);
                  //left = arithmetic.add(left, right);
                continue;
              }
              if ("-".equals(which)) {
//                Binary b = new Binary();
//                b.op = "-";
//                b.left = left;
//                b.right = right;
//                left = b;
                left = newBinary(ExpOp.SUBTRACT, left, right);
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
    System.out.println("RET LEFT: " + left);
    return left;
  }

  @Override
  public Object visit(ASTAdditiveOperator node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTMulNode node, Object data) {
    return newBinary(ExpOp.MULTIPLY, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTDivNode node, Object data) {
    return newBinary(ExpOp.DIVIDE, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTModNode node, Object data) {
    return newBinary(ExpOp.MOD, 
        node.jjtGetChild(0).jjtAccept(this, data),
        node.jjtGetChild(1).jjtAccept(this, data));
  }

  @Override
  public Object visit(ASTUnaryMinusNode node, Object data) {
    Object obj = node.jjtGetChild(0).jjtAccept(this, data);
    if (obj instanceof NumericLiteral) {
      ((NumericLiteral) obj).number = "-" + ((NumericLiteral) obj).number;
      return obj;
    } else if (obj instanceof ExpNodeConfig) {
      ((ExpNodeConfig) obj).setNegate(true);
      return obj;
    } else if (obj instanceof Bool) {
      // weird but parser allows it
      ((Bool) obj).bool = !((Bool) obj).bool;
      return obj;
    } else {
      throw new RuntimeException("Can't negate " + obj.getClass());
    }
  }

  @Override
  public Object visit(ASTBitwiseComplNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTNotNode node, Object data) {
    Object obj = node.jjtGetChild(0).jjtAccept(this, data);
    if (obj instanceof NumericLiteral) {
      // weird but parser allows it
      ((NumericLiteral) obj).number = "-" + ((NumericLiteral) obj).number;
      return obj;
    } else if (obj instanceof Bool) {
      ((Bool) obj).bool = !((Bool) obj).bool;
      return obj;
    } else if (obj instanceof ExpNodeConfig) {
      ((ExpNodeConfig) obj).setNot(true);
      return obj;
    } else {
      throw new RuntimeException("Can't negate " + obj.getClass());
    }
  }

  @Override
  public Object visit(ASTIdentifier node, Object data) {
    return new Identifier(node.image);
  }

  @Override
  public Object visit(ASTNullLiteral node, Object data) {
    return new Null();
  }

  @Override
  public Object visit(ASTTrueNode node, Object data) {
    return new Bool(true);
  }

  @Override
  public Object visit(ASTFalseNode node, Object data) {
    return new Bool(false);
  }

  @Override
  public Object visit(ASTNumberLiteral node, Object data) {
    return new NumericLiteral(node.image);
  }

  @Override
  public Object visit(ASTStringLiteral node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTArrayLiteral node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTMapLiteral node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTMapEntry node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTEmptyFunction node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTSizeFunction node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTFunctionNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTMethodNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTSizeMethod node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTConstructorNode node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTArrayAccess node, Object data) {
    throw new UnsupportedOperationException(node.getClass() 
        + " is not supported for expressions.");
  }

  @Override
  public Object visit(ASTReferenceExpression node, Object data) {
    return node.jjtGetChild(0).jjtAccept(this, data);
  }

}
