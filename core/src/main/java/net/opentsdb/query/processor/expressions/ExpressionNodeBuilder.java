package net.opentsdb.query.processor.expressions;

import java.io.StringReader;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;

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
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_operands_ruleContext;
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
import net.opentsdb.query.BaseQueryNodeConfig;
import net.opentsdb.query.QueryNodeConfig;

public class ExpressionNodeBuilder implements MetricExpressionVisitor<Object> {

  static enum BranchType {
    VARIABLE,
    SUB_EXP,
    LITERAL_NUMERIC,
    LITERAL_STRING,
    LITERAL_BOOL,
    NULL
  }
  
  static enum ExpOp {
    OR(new String[] { "||", "OR" }),
    AND(new String[] { "&&", "AND" }),
    EQ(new String[] { "==" }),
    NE(new String[] { "!=" }),
    LT(new String[] { "<" }),
    GT(new String[] { ">" }),
    LE(new String[] { "<=" }),
    GE(new String[] { ">=" }),
    ADD(new String[] { "+" }),
    SUBTRACT(new String[] { "-" }),
    MULTIPLY(new String[] { "*" }),
    DIVIDE(new String[] { "/" }),
    MOD(new String[] { "%" });
    
    private final String[] symbols;
    
    ExpOp(final String[] symbols) {
      this.symbols = symbols;
    }
    
    static public ExpOp parse(final String symbol) {
      for (int i = 0; i < values().length; i++) {
        final ExpOp op = values()[i];
        for (final String op_symbol : op.symbols) {
          if (op_symbol.equals(symbol)) {
            return op;
          }
        }
      }
      throw new RuntimeException("Unrecognized symbol: " + symbol);
    }
  }
  
  static class ExpNodeConfig extends BaseQueryNodeConfig {
    String left;
    BranchType left_type;
    String right;
    BranchType right_type;
    ExpOp op;
    boolean negate;
    boolean not;
    
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
    
    MetricExpressionLexer lexer = new MetricExpressionLexer(new ANTLRInputStream(exp));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    MetricExpressionParser parser = new MetricExpressionParser(tokens);
    nodes.add((ExpNodeConfig) parser.prog().accept(this));
//    JexlInfo debug = new DebugInfo(null, 0, 0);
//    StringReader rdr = new StringReader(exp);
//    Parser parser = new Parser(rdr);
//    try {
//      ASTJexlScript script = parser.parse(rdr, debug);
//      if (script.jjtGetNumChildren() > 1) {
//        throw new RuntimeException("WTF? A script with more than one root???");
//      }
//      Object obj = script.jjtAccept(this, null);
//      if (obj instanceof ExpNodeConfig) {
//        //nodes.add((ExpNodeConfig) obj);
//      } else {
//        System.out.println("Root node can't be of type: " + obj.getClass());
//      }
//    } catch (ParseException e) {
//      System.out.println("DEBUG: " + debug.debugString());
//      throw new JexlException.Parsing(debug, exp, e);
//    }
    
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
    } else if (obj instanceof String) {
      // handle the funky "escape keywords" case. e.g. "sys.'if'.out"
      if (is_left) {
        builder.setLeft((String) obj)
               .setLeftType(BranchType.VARIABLE);
      } else {
        builder.setRight((String) obj)
               .setRightType(BranchType.VARIABLE);
      }
    } else {
      throw new RuntimeException("NEED TO HANDLE: " + obj.getClass());
    }
  }

  @Override
  public Object visit(ParseTree tree) {
    throw new UnsupportedOperationException("Can't visit " 
        + tree.getClass());
  }

  @Override
  public Object visitChildren(RuleNode node) {
    throw new UnsupportedOperationException("Can't visit " 
        + node.getClass());
  }

  @Override
  public Object visitTerminal(TerminalNode node) {
    // this is a variable or operand (or parens).
    return node.getText();
  }

  @Override
  public Object visitErrorNode(ErrorNode node) {
    throw new RuntimeException("Error parsing: " + node.getText());
  }

  @Override
  public Object visitProg(ProgContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitArithmetic(ArithmeticContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitLogical(LogicalContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitRelational(RelationalContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitParen_logical_rule(Paren_logical_ruleContext ctx) {
    // catch exceptions
    ctx.getChild(0).accept(this);
    ctx.getChild(2).accept(this);
    return ctx.getChild(1).accept(this);
  }

  @Override
  public Object visitLogical_operands_rule(Logical_operands_ruleContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitLogical_expr_and_rule(Logical_expr_and_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitLogical_expr_or_rule(Logical_expr_or_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitLogical_expr_not_rule(Logical_expr_not_ruleContext ctx) {
    final Object child = ctx.getChild(1).accept(this);
    if (child instanceof ExpNodeConfig) {
      ((ExpNodeConfig) child).not = true;
    } else {
      throw new RuntimeException("Response from the child of the not was a " + child.getClass());
    }
    return child;
  }

  @Override
  public Object visitLogicalOperands(LogicalOperandsContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitArith_operands_rule(Arith_operands_ruleContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitParen_arith_rule(Paren_arith_ruleContext ctx) {
    // catch errors
    ctx.getChild(0).accept(this);
    ctx.getChild(2).accept(this);
    return ctx.getChild(1).accept(this);
  }

  @Override
  public Object visitMinus_metric_rule(Minus_metric_ruleContext ctx) {
    final Object child = ctx.getChild(1).accept(this);
    if (child instanceof ExpNodeConfig) {
      ((ExpNodeConfig) child).negate = true;
    } else {
      throw new RuntimeException("Response from the child of the minus was a " + child.getClass());
    }
    return child;
  }

  @Override
  public Object visitMod_arith_rule(Mod_arith_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitAddsub_arith_rule(Addsub_arith_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitDivmul_arith_rule(Divmul_arith_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitArithmetic_operands_rule(
      Arithmetic_operands_ruleContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitParen_relational_rule(Paren_relational_ruleContext ctx) {
    // catch exceptions
    ctx.getChild(0).accept(this);
    ctx.getChild(2).accept(this);
    return ctx.getChild(1).accept(this);
  }

  @Override
  public Object visitMain_relational_rule(Main_relational_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitRelational_operands_rule(
      Relational_operands_ruleContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitLogicop(LogicopContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitRelationalop(RelationalopContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitAnd(AndContext ctx) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object visitOr(OrContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitNot(NotContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitModulo(ModuloContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitMetric(MetricContext ctx) {
    return ctx.getChild(0).accept(this);
  }
  
}
