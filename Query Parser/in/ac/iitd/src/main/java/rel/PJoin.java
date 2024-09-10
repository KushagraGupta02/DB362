package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.units.qual.A;

import java.util.*;
import java.util.function.BiFunction;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.stream.Stream;

/*
 * Implement Hash Join
 * The left child is blocking, the right child is streaming
 */
public class PJoin extends Join implements PRel {

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }

    private Integer[] left_join_keys;
    private Integer[] right_join_keys;
    private HashMap<List<Object>, List<Object[]>> left_hashed_records;
    private HashMap<List<Object>, List<Object[]>> right_hashed_records;
    private List<Object[]> streaming_right_array;
    private List<Object[]> blocking_left_array;

    private HashMap<List<Object>, Integer> leftIndex_tracker;
    private HashMap<List<Object>, Integer> rightIndex_tracker;
    private int right_table_index;
    private int left_table_index;
    private int left_size;
    private int right_size;
    private int my_join_type;
    Object my_index = evaluate(condition);

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        if (!openChildRelations()) {
            return false;
        }
        initDS();
        extractJoinKeys();
        fetchAndIndexLeftRows();
        fetchAndIndexRightRows();
        Object new_index;
        if (my_index instanceof Integer) {
            new_index = my_index;
        } else if (my_index instanceof Boolean) {
            new_index = my_index;
        }
        return true;
    }

    private boolean openChildRelations() {
        boolean b = ((PRel) getRight()).open();
        boolean a = ((PRel) getLeft()).open();
        return a || b;
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
    public Object evaluate(RexNode filter_expression) {
        if (filter_expression instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) filter_expression;
            return inputRef.getIndex();
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
                return evaluate(operand);
            }

            RexCall call = (RexCall) filter_expression;
            List<RexNode> operands = call.getOperands();
            List<Object> evaluatedOperands = new ArrayList<>(operands.size());
            for (RexNode operand : operands) {
                evaluatedOperands.add(evaluate(operand));
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
                case IS_NULL:
                    RexNode operand = call.getOperands().get(0);
                    Object val = evaluate(operand);
                    if (val == null){
                        return true;
                    }
                    return false;
                case IS_NOT_NULL:
                    RexNode operand2 = call.getOperands().get(0);
                    Object val2 = evaluate(operand2);
                    if (val2 == null){
                        return false;
                    }
                    return true;

                default:
                    throw new UnsupportedOperationException("Unsupported operator: " + kind);
            }
        }
        return null;
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

    private List<Object> left_key_hash(Object[] row) {
        List<Object> hash = new ArrayList<>();
        List<Object> hash_opp = new ArrayList<>();
        int i = 0;
        while (i < left_join_keys.length){
            hash.add(row[left_join_keys[i]]);
            hash_opp.add(row[left_join_keys[left_join_keys.length-i-1]]);
            i++;
        }
        return hash;
    }

    private List<Object> right_key_hash(Object[] row) {
        List<Object> hash = new ArrayList<>();
        List<Object> hash_opp = new ArrayList<>();
        int i = 0;
        while (i < right_join_keys.length){
            hash.add(row[right_join_keys[i]]);
            hash_opp.add(row[right_join_keys[right_join_keys.length-i-1]]);
            i++;
        }
        return hash;
    }
    private void fetchAndIndexLeftRows() {
        while (((PRel) getLeft()).hasNext()) {
            Object[] new_row = ((PRel) getLeft()).next();
            left_size = new_row.length;
            Stream<Object> leftStream = Arrays.stream(new_row);
            blocking_left_array.add(new_row);
            List<Object> hash = left_key_hash(new_row);
            Iterator<Object> leftIter = leftStream.iterator();
            if (left_hashed_records.containsKey(hash)) {
                left_hashed_records.get(hash).add(new_row);
            }
            else{ List<Object[]> new_hash_list = new ArrayList<>();
                new_hash_list.add(new_row);
                leftIndex_tracker.put(hash, 0);
                left_hashed_records.put(hash, new_hash_list);}
        }
    }

    private void fetchAndIndexRightRows() {
        while (((PRel) getRight()).hasNext()) {
            Object[] new_row = ((PRel) getRight()).next();
            right_size = new_row.length;
            Stream<Object> rightStream = Arrays.stream(new_row);
            streaming_right_array.add(new_row);
            List<Object> hash = right_key_hash(new_row);
            Iterator<Object> rightIter = rightStream.iterator();
            if (right_hashed_records.containsKey(hash)) {
                right_hashed_records.get(hash).add(new_row);
            } else {
                List<Object[]> new_hash_list = new ArrayList<>();
                new_hash_list.add(new_row);
                rightIndex_tracker.put(hash, 0);
                right_hashed_records.put(hash, new_hash_list);
            }
        }
    }

    private void fillWithNulls(Object[] array, int start, int length) {
        for (int i = start; i < start + length; i++) {
            array[i] = null;
        }
    }

    private void extractJoinKeys() {
        left_join_keys = joinInfo.leftKeys.toArray(new Integer[0]);
        right_join_keys = joinInfo.rightKeys.toArray(new Integer[0]);
    }

    private void initDS(){
        leftIndex_tracker = new HashMap<>();
        rightIndex_tracker = new HashMap<>();
        right_table_index = 0;
        left_table_index = 0;
        left_hashed_records = new HashMap<>();
        streaming_right_array = new ArrayList<>();
        blocking_left_array = new ArrayList<>();
        right_hashed_records = new HashMap<>();
        my_join_type = 0;
    }


    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        /* Write your code here */
        right_table_index = 0;
        left_table_index = 0;
        my_join_type = 0;
        ((PRel) getLeft()).close();
        ((PRel) getRight()).close();
        leftIndex_tracker.clear();
        rightIndex_tracker.clear();
        left_hashed_records.clear();
        blocking_left_array.clear();
        right_hashed_records.clear();
        streaming_right_array.clear();
        return;
    }

    public boolean processLeftJoin(){
        while(left_table_index < blocking_left_array.size()){
            Object[] leftRow = blocking_left_array.get(left_table_index);
            List<Object> left_hashed_key = left_key_hash(leftRow);
            if(!right_hashed_records.containsKey(left_hashed_key)){
                return true;
            }
            List<Object[]> rightRows = right_hashed_records.get(left_hashed_key);
            int get_index = rightIndex_tracker.getOrDefault(left_hashed_key,0);
            if(!(get_index == rightRows.size())){
                return true;
            }
            else{
                left_table_index ++;
                get_index = 0;
                rightIndex_tracker.put(left_hashed_key,get_index);
            }
        }
        return false;
    }

    public boolean processOuterJoin(){
        while(left_table_index < blocking_left_array.size()){
            Object[] leftRow = blocking_left_array.get(left_table_index);
            List<Object> left_hashed_key = left_key_hash(leftRow);
            if(!right_hashed_records.containsKey(left_hashed_key)){
                my_join_type = -1;
                return true;
            }
            List<Object[]> rightRows = right_hashed_records.get(left_hashed_key);
            int get_index = rightIndex_tracker.getOrDefault(left_hashed_key,0);
            if(!(get_index == rightRows.size())){
                my_join_type = 0;
                return true;
            }
            else{
                left_table_index++;
                get_index = 0;
                rightIndex_tracker.put(left_hashed_key,get_index);
            }
        }
        while(right_table_index < streaming_right_array.size()){
            Object[] rightRow = streaming_right_array.get(right_table_index);
            List<Object> right_hashed_key = right_key_hash(rightRow);
            if(left_hashed_records.containsKey(right_hashed_key)){
                right_table_index++;
            }
            else{
                my_join_type = 1;
                return true;
            }
        }
        return false;
    }
    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        /* Write your code here */
        if(joinType == JoinRelType.FULL){
            return processOuterJoin();
        }
        if(joinType == JoinRelType.LEFT){
            return processLeftJoin();
        }
        while(right_table_index < streaming_right_array.size()){
            Object[] rightRow = streaming_right_array.get(right_table_index);
            List<Object> right_hashed_key = right_key_hash(rightRow);
            if(!left_hashed_records.containsKey(right_hashed_key)){
                if((JoinRelType)joinType == JoinRelType.RIGHT){
                    return true;
                }
                right_table_index++;
                continue;
            }
            List<Object[]> leftRows = left_hashed_records.get(right_hashed_key);
            int get_index = leftIndex_tracker.getOrDefault(right_hashed_key,0);
            if(get_index == leftRows.size()){
                right_table_index ++;
                get_index = 0;
                leftIndex_tracker.put(right_hashed_key,get_index);
            }
            else{
                return true;
            }
        }
        return false;
    }

    Object [] processNextRowForLeftJoin(){
        Object[] leftRow = blocking_left_array.get(left_table_index);
        List<Object> left_hashed_key = left_key_hash(leftRow);
        Object[] joinedRow = new Object[left_size + right_size];
        if (!right_hashed_records.containsKey(left_hashed_key)) {
            left_table_index++;
            fillWithNulls(joinedRow, left_size, right_size);
            System.arraycopy(leftRow, 0, joinedRow, 0, left_size);
            return joinedRow;
        }
        List<Object[]> rightRows = right_hashed_records.get(left_hashed_key);
        int get_index = rightIndex_tracker.getOrDefault(left_hashed_key,0);

        Object[] rightRow = rightRows.get(get_index);

        System.arraycopy(leftRow,0,joinedRow,0,left_size);
        System.arraycopy(rightRow,0,joinedRow,left_size,right_size);
        get_index++;
        rightIndex_tracker.put(left_hashed_key,get_index);
        return joinedRow;
    }
    Object [] processNextRowForOuterJoin(){
        Object[] joinedRow = new Object[left_size + right_size];
        if(my_join_type == 1){
            Object[] rightRow = streaming_right_array.get(right_table_index);
            right_table_index++;
            Object right_row_new = null;
            if (left_table_index < left_hashed_records.size() && left_table_index > 0) {
                right_row_new = left_hashed_records.get(left_table_index);
            }
            fillWithNulls(joinedRow, 0, left_size);
            System.arraycopy(rightRow,0,joinedRow,left_size, right_size);
            return joinedRow;
        }
        else if(my_join_type == -1){
            Object[] leftRow = blocking_left_array.get(left_table_index);
            left_table_index++;
            Object left_temp_row = null;
            if (left_table_index < left_hashed_records.size() && left_table_index > 0) {
                left_temp_row = left_hashed_records.get(left_table_index);
            }
            fillWithNulls(joinedRow, left_size, right_size);
            System.arraycopy(leftRow,0,joinedRow,0,left_size);
            return joinedRow;
        }
        else{
            Object[] leftRow = blocking_left_array.get(left_table_index);
            List<Object> left_hashed_key = left_key_hash(leftRow);
            List<Object[]> rightRows = right_hashed_records.get(left_hashed_key);
            int get_index = rightIndex_tracker.getOrDefault(left_hashed_key,0);
            Object[] rightRow = rightRows.get(get_index);

            System.arraycopy(leftRow,0,joinedRow,0,left_size);
            System.arraycopy(rightRow,0,joinedRow,left_size,right_size);
            get_index++;
            rightIndex_tracker.put(left_hashed_key,get_index);
            return joinedRow;
        }
    }
    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        /* Write your code here */
        try {
            if(!this.hasNext()) return null;
            if(joinType == JoinRelType.FULL){
                return processNextRowForOuterJoin();
            }
            if(joinType == JoinRelType.LEFT){
                return processNextRowForLeftJoin();
            }
            Object[] rightRow = streaming_right_array.get(right_table_index);
            List<Object> right_hashed_key = right_key_hash(rightRow);
            Object[] joinedRow = new Object[left_size + right_size];
            if(joinType == JoinRelType.RIGHT) {
                if (!left_hashed_records.containsKey(right_hashed_key)) {
                    fillWithNulls(joinedRow, 0, left_size);
                    System.arraycopy(rightRow, 0, joinedRow, left_size, right_size);
                    right_table_index++;
                    return joinedRow;
                }
            }
            List<Object[]> leftRows = left_hashed_records.get(right_hashed_key);
            int get_index = leftIndex_tracker.get(right_hashed_key);

            Object[] leftRow = leftRows.get(get_index);
            System.arraycopy(rightRow,0,joinedRow,left_size,right_size);
            System.arraycopy(leftRow,0,joinedRow,0,left_size);

            get_index++;
            leftIndex_tracker.put(right_hashed_key,get_index);
            return joinedRow;
        } catch (Exception e) {
            logger.error("An unexpected error occurred while processing the next row.", e);
            return null;
        }
    }

}
