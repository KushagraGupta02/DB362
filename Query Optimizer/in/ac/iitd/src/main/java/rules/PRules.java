package rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

import convention.PConvention;
import rel.PProjectFilter;
import rel.PTableScan;

import org.checkerframework.checker.nullness.qual.Nullable;


public class PRules {

    private PRules(){
    }

    public static final RelOptRule P_TABLESCAN_RULE = new PTableScanRule(PTableScanRule.DEFAULT_CONFIG);

    private static class PTableScanRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalTableScan.class,
                        Convention.NONE, PConvention.INSTANCE,
                        "PTableScanRule")
                .withRuleFactory(PTableScanRule::new);

        protected PTableScanRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode relNode) {

            TableScan scan = (TableScan) relNode;
            final RelOptTable relOptTable = scan.getTable();

            if(relOptTable.getRowType() == scan.getRowType()) {
                return PTableScan.create(scan.getCluster(), relOptTable);
            }

            return null;
        }
    }

    // Write a class PProjectFilterRule that converts a LogicalProject followed by a LogicalFilter to a single PProjectFilter node.
    public static class PProjectFilterRule extends RelOptRule {

        protected PProjectFilterRule() {
            super(operand(LogicalProject.class, operand(LogicalFilter.class, any())), "PProjectFilterRule");
        }

        public static final PProjectFilterRule INSTANCE = new PProjectFilterRule();


        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalProject myproject = call.rel(0);
            final LogicalFilter myfilter = call.rel(1);

            final RelNode output = new PProjectFilter(
                    myproject.getCluster(),
                    myfilter.getTraitSet().replace(PConvention.INSTANCE),
                    convert(myfilter.getInput(), myfilter.getInput().getTraitSet().replace(PConvention.INSTANCE)
                    ), myproject.getProjects(), myproject.getRowType(), myfilter.getCondition());

            call.transformTo(output);
        }
    }

}
