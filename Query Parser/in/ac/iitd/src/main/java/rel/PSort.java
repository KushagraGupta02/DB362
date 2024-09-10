package rel;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexLiteral;

import convention.PConvention;
import org.apache.calcite.sql.type.SqlTypeName;

public class PSort extends Sort implements PRel{
    public List<Object[]> sortedRows;
    public int currentIndex;
    public  int offsetValue;
    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
    ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        if (((PRel) input).open()) {
            sortedRows = new ArrayList<>();
            Object[] row;
            while (((PRel) input).hasNext()) {
                row = ((PRel) input).next();
                sortedRows.add(row);
            }
            sortRows();
            offsetValue = offset != null ? RexLiteral.intValue(offset) : 0;
            currentIndex = offsetValue;
            return true;
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        ((PRel) input).close();
        sortedRows.clear();
        currentIndex = 0;
    }

    public int comparator(Object first, Object second, SqlTypeName type) {
        if(first instanceof Double || second instanceof Double){
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

    public void sortRows() {
        logger.trace("Sorting rows");
        List<RelFieldCollation> fieldCollations = getCollation().getFieldCollations();
        sortedRows.sort((row1, row2) -> {
            for (RelFieldCollation fieldCollation : fieldCollations) {
                int ordinal = fieldCollation.getFieldIndex();
                boolean ascending = fieldCollation.getDirection().isDescending();
                SqlTypeName typeName = getRowType().getFieldList().get(ordinal).getType().getSqlTypeName();
                if (row1[ordinal] == null && row2[ordinal] == null) {
                    return 0;
                }
                else if (row1[ordinal] == null) {
                    return 1;
                }
                else if (row2[ordinal] == null) {
                    return -1;
                }
                int comparison = comparator(row1[ordinal], row2[ordinal], typeName);
                if (ascending) {
                    comparison = -comparison;
                }
                if (comparison != 0) {
                    return comparison;
                }
            }
            return 0;
        });
    }
    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        int fetchValue = fetch != null ? RexLiteral.intValue(fetch) : Integer.MAX_VALUE;
        return currentIndex < sortedRows.size() && currentIndex - offsetValue < fetchValue;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        if (hasNext()) {
            return sortedRows.get(currentIndex++);
        }
        return null;
    }

}
