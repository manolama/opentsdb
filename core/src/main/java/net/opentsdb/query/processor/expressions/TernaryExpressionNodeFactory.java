package net.opentsdb.query.processor.expressions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.processor.BaseQueryNodeFactory;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

public class TernaryExpressionNodeFactory extends BaseQueryNodeFactory<
    TernaryExpressionParseNode, TernaryExpressionNode> {
  public static final String TYPE = "TernaryExpression";
  
  @Override
  public TernaryExpressionParseNode parseConfig(final ObjectMapper mapper, 
                                      final TSDB tsdb,
                                      final JsonNode node) {
    TernaryExpressionParseNode.Builder builder = 
        TernaryExpressionParseNode.newBuilder();
    
    OperandType left_type;
    OperandType right_type;
    
    JsonNode n = node.get("leftType");
    if (n == null) {
      throw new IllegalArgumentException("Node must have the left type.");
    }
    try {
      left_type = mapper.treeToValue(n, OperandType.class);
      builder.setLeftType(left_type);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse left type.", e);
    }
    
    n = node.get("rightType");
    if (n == null) {
      throw new IllegalArgumentException("Node must have the right type.");
    }
    try {
      right_type = mapper.treeToValue(n, OperandType.class);
      builder.setRightType(right_type);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse right type.", e);
    }
    
    n = node.get("left");
    if (n == null && left_type != OperandType.NULL) {
      throw new IllegalArgumentException("Left operand cannot be null.");
    }
    
    switch(left_type) {
    case LITERAL_NUMERIC:
      if (NumericType.looksLikeInteger(n.asText())) {
        builder.setLeft(new ExpressionParser.NumericLiteral(n.asLong()));
      } else {
        builder.setLeft(new ExpressionParser.NumericLiteral(n.asDouble()));
      }
      break;
    default:
      builder.setLeft(n.asText());
    }
    
    n = node.get("right");
    if (n == null && right_type != OperandType.NULL) {
      throw new IllegalArgumentException("Right operand cannot be null.");
    }
    switch(right_type) {
    case LITERAL_NUMERIC:
      if (NumericType.looksLikeInteger(n.asText())) {
        builder.setRight(new ExpressionParser.NumericLiteral(n.asLong()));
      } else {
        builder.setRight(new ExpressionParser.NumericLiteral(n.asDouble()));
      }
      break;
    default:
      builder.setRight(n.asText());
    }
    
    n = node.get("operator");
    if (n == null) {
      throw new IllegalArgumentException("Operation cannot be null.");
    }
    try {
      builder.setExpressionOp( mapper.treeToValue(n, ExpressionOp.class));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to parse op.", e);
    }
    
    n = node.get("not");
    if (n != null) {
      builder.setNot(n.asBoolean());
    }
    
    n = node.get("negate");
    if (n != null) {
      builder.setNegate(n.asBoolean());
    }
    
    n = node.get("expressionConfig");
    if (n == null) {
      throw new IllegalArgumentException("The expressionConfig cannot "
          + "be null.");
    }
    builder.setExpressionConfig(ExpressionConfig.parse(mapper, tsdb, n));
    
    n = node.get("as");
    if (n != null && !n.isNull()) {
      builder.setAs(n.asText());
    }
    
    n = node.get("id");
    if (n == null || n.isNull()) {
      throw new IllegalArgumentException("Missing ID for node config");
    }
    builder.setId(n.asText());
    
    return builder.build();
  }

  @Override
  public TernaryExpressionNode newNode(final QueryPipelineContext context,
                                       final TernaryExpressionParseNode config) {
    return new TernaryExpressionNode(this, context, config);
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    // TODO Auto-generated method stub
    return null;
  }

}
