package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                         List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        /* Write your code here */
        return ((PRel) input).open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        /* Write your code here */
        ((PRel) input).close();
    }
    private Object performOperation(Object left, Object right, SqlKind kind) {
        if (left instanceof Integer && right instanceof Integer) {
            return performIntegerOperation((Integer) left, (Integer) right, kind);
        } else if (left instanceof Double && right instanceof Double) {
            return performDoubleOperation((Double) left, (Double) right, kind);
        } else if (left instanceof BigDecimal && right instanceof BigDecimal) {
            return performBigDecimalOperation((BigDecimal) left, (BigDecimal) right, kind);
        } else if (left instanceof String && right instanceof String) {
            return performStringOperation((String) left, (String) right, kind);
        } else {
            // Handle type mismatch or unsupported types
            throw new UnsupportedOperationException("Unsupported operand types: " + left.getClass().getName() + " and " + right.getClass().getName());
        }
    }

    private Object performIntegerOperation(Integer left, Integer right, SqlKind kind) {
        switch (kind) {
            case PLUS:
                return left + right;
            case MINUS:
                return left - right;
            case TIMES:
                return left * right;
            case DIVIDE:
                return (double) left / right;
            default:
                throw new UnsupportedOperationException("Unsupported operation for integers: " + kind);
        }
    }

    private Object performDoubleOperation(Double left, Double right, SqlKind kind) {
        switch (kind) {
            case PLUS:
                return left + right;
            case MINUS:
                return left - right;
            case TIMES:
                return left * right;
            case DIVIDE:
                return left / right;
            default:
                throw new UnsupportedOperationException("Unsupported operation for doubles: " + kind);
        }
    }

    private Object performBigDecimalOperation(BigDecimal left, BigDecimal right, SqlKind kind) {
        switch (kind) {
            case PLUS:
                return left.add(right);
            case MINUS:
                return left.subtract(right);
            case TIMES:
                return left.multiply(right);
            case DIVIDE:
                return left.divide(right, BigDecimal.ROUND_HALF_UP);
            default:
                throw new UnsupportedOperationException("Unsupported operation for BigDecimal: " + kind);
        }
    }

    private Object performStringOperation(String left, String right, SqlKind kind) {
        switch (kind) {
            case PLUS:
                return left + right;
            default:
                throw new UnsupportedOperationException("Unsupported operation for strings: " + kind);
        }
    }

    public int comparator(Object first, Object second, SqlTypeName type) {
        if(first instanceof Double || second instanceof Double || first instanceof BigInteger || second instanceof BigInteger){
            Double first1 = Double.parseDouble(first.toString());
            Double first2 = Double.parseDouble(second.toString());
            return Double.compare((Double) first1, (Double) first2);
        }
        if (first instanceof Integer && second instanceof Integer) {
            return Integer.compare((Integer) first, (Integer) second);
        }
        if (type == SqlTypeName.INTEGER) {
            if (first instanceof BigDecimal) {
                first = ((BigDecimal) first).intValueExact();
            }
            if (second instanceof BigDecimal) {
                second = ((BigDecimal) second).intValueExact();
            }
            return Integer.compare((Integer) first, (Integer) second);
        } else if (type == SqlTypeName.VARCHAR) {
            return ((String) first).compareTo((String) second);
        } else if (type == SqlTypeName.CHAR) {
            return ((String) first).compareTo((String) second);
        } else if (type == SqlTypeName.DOUBLE) {
            return Double.compare((Double) first, (Double) second);
        } else if (type == SqlTypeName.DECIMAL) {
            if (first instanceof BigDecimal && second instanceof BigDecimal) {
                return ((BigDecimal) first).compareTo((BigDecimal) second);
            } else {
                return Double.compare(((BigDecimal) first).doubleValue(), ((BigDecimal) second).doubleValue());
            }
        } else if (type == SqlTypeName.BOOLEAN) {
            boolean a = (Boolean) first;
            boolean b = (Boolean) second;
            if (a == b) {
                return 0;
            } else if (a) {
                return 1;
            } else {
                return -1;
            }
        } else if (type == SqlTypeName.FLOAT) {
            return Float.compare((Float) first, (Float) second);
        }
        return 0;
    }

    private SqlTypeName getType(RexNode node) {
        if (node instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) node;
            return getRowType().getFieldList().get(inputRef.getIndex()).getType().getSqlTypeName();
        } else if (node instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) node;
            return literal.getType().getSqlTypeName();
        }
        throw new UnsupportedOperationException("Unsupported node type for getting type: " + node.getClass().getName());
    }
    public Object evaluate(RexNode filter_expression, Object[] row) {
        if (filter_expression instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) filter_expression;
            return row[inputRef.getIndex()];
        } else if (filter_expression instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) filter_expression;
            SqlTypeName typeName = literal.getType().getSqlTypeName();
            Object value;
            switch (typeName) {
                case CHAR:
                case VARCHAR:
                    value = literal.getValue2();
                    break;
                case DECIMAL:
                case DOUBLE:
                    value = literal.getValueAs(Double.class);
                    break;
                case FLOAT:
                    value = literal.getValueAs(Float.class);
                    break;
                case BOOLEAN:
                    value = literal.getValueAs(Boolean.class);
                    break;
                case INTEGER:
                    value = literal.getValueAs(Integer.class);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported operator: ");
            }
            return value;
        } else if (filter_expression instanceof RexCall) {
            if (filter_expression.isA(SqlKind.CAST)) {
                RexNode operand =   ((RexCall) filter_expression).getOperands().get(0);
                return evaluate(operand,row);
            }
            RexCall call = (RexCall) filter_expression;
            List<RexNode> operands = call.getOperands();
            List<Object> evaluatedOperands = new ArrayList<>(operands.size());
            for (RexNode operand : operands) {
                evaluatedOperands.add(evaluate(operand, row));
            }
            Object operand0 = evaluatedOperands.get(0);
            SqlTypeName typeName = null;
            if (operand0 instanceof String) {
                typeName = SqlTypeName.VARCHAR;
            } else if (operand0 instanceof Integer) {
                typeName = SqlTypeName.INTEGER;
            } else if (operand0 instanceof Double) {
                typeName = SqlTypeName.DOUBLE;
            } else if (operand0 instanceof Float) {
                typeName = SqlTypeName.FLOAT;
            } else if (operand0 instanceof BigDecimal) {
                typeName = SqlTypeName.DECIMAL;
            } else if (operand0 instanceof Boolean) {
                typeName = SqlTypeName.BOOLEAN;
            }
            SqlKind kind = call.getKind();
            switch (kind) {
                case EQUALS:
                    return comparator(evaluatedOperands.get(0), evaluatedOperands.get(1), typeName) == 0;
                case NOT_EQUALS:
                    return comparator(evaluatedOperands.get(0), evaluatedOperands.get(1), typeName) != 0;
                case GREATER_THAN:
                    return comparator(evaluatedOperands.get(0), evaluatedOperands.get(1), typeName) > 0;
                case LESS_THAN:
                    return comparator(evaluatedOperands.get(0), evaluatedOperands.get(1), typeName) < 0;
                case GREATER_THAN_OR_EQUAL:
                    return comparator(evaluatedOperands.get(0), evaluatedOperands.get(1), typeName) >= 0;
                case LESS_THAN_OR_EQUAL:
                    return comparator(evaluatedOperands.get(0), evaluatedOperands.get(1), typeName) <= 0;
                case PLUS:
                case MINUS:
                case TIMES:
                case DIVIDE:
                    return arithmetic(evaluatedOperands, kind, typeName);
                case AND:
                    return logical(evaluatedOperands, (a, b) -> (boolean) a && (boolean) b);
                case OR:
                    return logical(evaluatedOperands, (a, b) -> (boolean) a || (boolean) b);
                // Add more cases for other operators as needed
                default:
                    throw new UnsupportedOperationException("Unsupported operator: " + kind);
            }
        }
        throw new UnsupportedOperationException("Unsupported expression type: " + filter_expression.getClass().getName());
    }

    private Object arithmetic(List<Object> operands, SqlKind operator, SqlTypeName type) {
        Object result = operands.get(0);
        for (int i = 1; i < operands.size(); i++) {
            result = performArithmetic(result, operands.get(i), operator, type);
        }
        return result;
    }


    private Object performArithmetic(Object a, Object b, SqlKind operator, SqlTypeName type) {
        if (a instanceof BigInteger || b instanceof BigInteger || a instanceof Double || b instanceof Double){
            Double aDouble = Double.parseDouble(a.toString());
            Double bDouble = Double.parseDouble(b.toString());
            switch (operator) {
                case PLUS:
                    return aDouble + bDouble;
                case MINUS:
                    return aDouble - bDouble;
                case TIMES:
                    return aDouble * bDouble;
                case DIVIDE:
                    return aDouble / bDouble;
                default:
                    throw new UnsupportedOperationException("Unsupported operator: " + operator);
            }
        }
        switch (type) {
            case INTEGER:
                int aInt = (int) a;
                int bInt = (int) b;
                switch (operator) {
                    case PLUS:
                        return aInt + bInt;
                    case MINUS:
                        return aInt - bInt;
                    case TIMES:
                        return aInt * bInt;
                    case DIVIDE:
                        return aInt / bInt;
                    default:
                        throw new UnsupportedOperationException("Unsupported operator: " + operator);
                }
            case DOUBLE:
                double aDouble = (double) a;
                double bDouble = (double) b;
                switch (operator) {
                    case PLUS:
                        return aDouble + bDouble;
                    case MINUS:
                        return aDouble - bDouble;
                    case TIMES:
                        return aDouble * bDouble;
                    case DIVIDE:
                        return aDouble / bDouble;
                    default:
                        throw new UnsupportedOperationException("Unsupported operator: " + operator);
                }
            case FLOAT:
                float aFloat = (float) a;
                float bFloat = (float) b;
                switch (operator) {
                    case PLUS:
                        return aFloat + bFloat;
                    case MINUS:
                        return aFloat - bFloat;
                    case TIMES:
                        return aFloat * bFloat;
                    case DIVIDE:
                        return aFloat / bFloat;
                    default:
                        throw new UnsupportedOperationException("Unsupported operator: " + operator);
                }
            case BOOLEAN:
                boolean aBoolean = (boolean) a;
                boolean bBoolean = (boolean) b;
                switch (operator) {
                    case AND:
                        return aBoolean && bBoolean;
                    case OR:
                        return aBoolean || bBoolean;
                    default:
                        throw new UnsupportedOperationException("Unsupported operator: " + operator);
                }
            case CHAR:
            case VARCHAR:
                String aString = (String) a;
                String bString = (String) b;
                switch (operator) {
                    case PLUS:
                        return aString + bString;
                    default:
                        throw new UnsupportedOperationException("Unsupported operator: " + operator);
                }
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private Object logical(List<Object> operands, BiFunction<Object, Object, Boolean> operation) {
        boolean result = (boolean) operands.get(0);
        for (int i = 1; i < operands.size(); i++) {
            result = operation.apply(result, (boolean) operands.get(i));
        }
        return result;
    }
    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        return ((PRel) input).hasNext();
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PProject");
        Object[] inputRow = ((PRel) input).next();
        if (inputRow == null) {
            return null;
        }
        Object[] projectedRow = new Object[getProjects().size()];

        for (int i = 0; i < getProjects().size(); i++) {
            RexNode project_expression = getProjects().get(i);
            projectedRow[i] = evaluate(project_expression, inputRow);
        }
        return projectedRow;
    }
}
