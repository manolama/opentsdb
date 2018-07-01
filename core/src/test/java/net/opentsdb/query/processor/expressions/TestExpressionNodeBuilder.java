package net.opentsdb.query.processor.expressions;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestExpressionNodeBuilder {

  @Test
  public void foo() throws Exception {
    ExpressionNodeBuilder b = new ExpressionNodeBuilder();
    b.parse("a + b + c", "exp1");
    
    System.out.println(b.nodes);
  }

  @Test
  public void epsilon() throws Exception {
    System.out.println(Math.ulp(1.0D));
    //assertEquals(1.0D, 2.0D, 0.001);
    
    System.out.println(Double.compare(1.0D, 2.0D));
  }
}
