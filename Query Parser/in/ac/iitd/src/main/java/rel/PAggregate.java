package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import convention.PConvention;

import java.util.*;

import static org.apache.calcite.util.Optionality.MANDATORY;


// Count, Min, Max, Sum, Avg
public class  PAggregate extends Aggregate implements PRel {
    public List<Object[]> inputRows;
    public List<Object[]> aggregatedRows;
    public int currentIndex;
    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }

    public Map<List<Object>, List<Object[]>> groupInputRows(List<Object[]> inputRows, ImmutableBitSet groupSet) {
        Map<List<Object>, List<Object[]>> groups = new HashMap<>();

        for (Object[] row : inputRows) {
            List<Object> groupKey = new ArrayList<>();
            for (int i = groupSet.nextSetBit(0); i >= 0; i = groupSet.nextSetBit(i + 1)) {
                groupKey.add(row[i]);
            }

            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(row);
        }

        return groups;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    public Object computeAggregate(AggregateCall aggCall, List<Object[]> groupRows) {
        SqlAggFunction aggFunction = aggCall.getAggregation();
        int aggArgIndex = aggCall.getArgList().isEmpty() ? -1 : aggCall.getArgList().get(0);

        switch (aggFunction.getKind()) {
            case COUNT:
                // count distinct
                if (aggCall.isDistinct()) {
                    Set<Object> distinctValues = new HashSet<>();
                    for (Object[] row : groupRows) {
                        Object value = row[aggArgIndex];
                        distinctValues.add(value); // Add value to the set
                    }
                    return distinctValues.size();
                }
                else
                    return groupRows.size();
            case SUM:
                if (aggCall.isDistinct()) {
                    Set<Object> distinctValues = new HashSet<>();
                    for (Object[] row : groupRows) {
                        Object value = row[aggArgIndex];
                        distinctValues.add(value); // Add value to the set
                    }
                    int sum = 0;
                    for (Object distinctValue : distinctValues) {
                        if (distinctValue instanceof Number) {
                            sum += ((Number) distinctValue).longValue();
                        }
                    }
                    return sum;
                }
                else{
                    int sum = 0;
                    for (Object[] row : groupRows) {
                        Object value = row[aggArgIndex];
                        if (value instanceof Number) {
                            sum += ((Number) value).longValue();
                        }
                    }
                    return sum;
                }
            case AVG:
                if (aggCall.isDistinct()) {
                    Set<Object> distinctValues = new HashSet<>();
                    for (Object[] row : groupRows) {
                        Object value = row[aggArgIndex];
                        if (value instanceof Number) {
                            distinctValues.add(value); // Add value to the set
                        }
                    }
                    long total = 0;
                    int count = 0;
                    for (Object distinctValue : distinctValues) {
                        total += ((Number) distinctValue).longValue();
                        count++;
                    }
                    return count > 0 ? (double) total / count : null;
                } else {
                    long total = 0;
                    int count = 0;
                    for (Object[] row : groupRows) {
                        Object value = row[aggArgIndex];
                        if (value instanceof Number) {
                            total += ((Number) value).longValue();
                            count++;
                        }
                    }
                    return count > 0 ? (double) total / count : null;
                }
            case MIN:
                Comparable min = null;
                for (Object[] row : groupRows) {
                    Object value = row[aggArgIndex];
                    if (value instanceof Comparable) {
                        if (min == null || ((Comparable) value).compareTo(min) < 0) {
                            min = (Comparable) value;
                        }
                    }
                }
                return min;
            case MAX:
                Comparable max = null;
                for (Object[] row : groupRows) {
                    Object value = row[aggArgIndex];
                    if (value instanceof Comparable) {
                        if (max == null || ((Comparable) value).compareTo(max) > 0) {
                            max = (Comparable) value;
                        }
                    }
                }
                return max;
            default:
                throw new UnsupportedOperationException("Unsupported aggregation function: " + aggFunction.getKind());
        }
    }

    public void performAggregation() {
        ImmutableBitSet groupSet = getGroupSet();
        List<ImmutableBitSet> groupSets = getGroupSets();
        List<AggregateCall> aggCalls = getAggCallList();

        // Group the input rows based on the groupSet
        Map<List<Object>, List<Object[]>> groups = groupInputRows(inputRows, groupSet);

        // Perform aggregations for each group
        for (Map.Entry<List<Object>, List<Object[]>> entry : groups.entrySet()) {
            List<Object> groupKey = entry.getKey();
            List<Object[]> groupRows = entry.getValue();

            Object[] aggregatedRow = new Object[groupKey.size()+aggCalls.size()];
            int aggregateIndex = 0;

            // Set the group-by values in the aggregated row
            for (int i = 0; i < groupKey.size(); i++) {
                aggregatedRow[aggregateIndex++] = groupKey.get(i);
            }

            // Perform aggregations for the group
            for (AggregateCall aggCall : aggCalls) {
                Object aggregateResult = computeAggregate(aggCall, groupRows);
                aggregatedRow[aggregateIndex++] = aggregateResult;
            }

            aggregatedRows.add(aggregatedRow);
        }
    }


    // returns true if successfully opened, false otherwise
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        if (((PRel) input).open()) {
            inputRows = new ArrayList<>();
            Object[] row;
            while (((PRel) input).hasNext()) {
                row = ((PRel) input).next();
                inputRows.add(row);
            }
            aggregatedRows = new ArrayList<>();
            performAggregation();
            currentIndex = 0;
            return true;
        }
        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        ((PRel) input).close();
        inputRows.clear();
        aggregatedRows.clear();
        currentIndex = 0;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        return currentIndex < aggregatedRows.size();
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        if (hasNext()) {
            return aggregatedRows.get(currentIndex++);
        }
        return null;
    }

}