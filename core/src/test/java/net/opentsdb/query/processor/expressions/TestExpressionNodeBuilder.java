package net.opentsdb.query.processor.expressions;

import org.junit.Test;

public class TestExpressionNodeBuilder {

  @Test
  public void foo() throws Exception {
    ExpressionNodeBuilder b = new ExpressionNodeBuilder();
    b.parse("a + b + c", "exp1");
    
    System.out.println(b.nodes);
  }
}
