package rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalSort;

import convention.PConvention;
import rel.*;
import org.checkerframework.checker.nullness.qual.Nullable;


public class PRules {

    private PRules(){
    }

    public static final RelOptRule P_PROJECT_RULE = new PProjectRule(PProjectRule.DEFAULT_CONFIG);
    public static final RelOptRule P_FILTER_RULE =  new PFilterRule(PFilterRule.DEFAULT_CONFIG);
    public static final RelOptRule P_TABLESCAN_RULE = new PTableScanRule(PTableScanRule.DEFAULT_CONFIG);
    public static final RelOptRule P_JOIN_RULE = new PJoinRule(PJoinRule.DEFAULT_CONFIG);
    public static final RelOptRule P_AGGREGATE_RULE = new PAggregateRule(PAggregateRule.DEFAULT_CONFIG);
    public static final RelOptRule P_SORT_RULE = new PSortRule(PSortRule.DEFAULT_CONFIG);

    private static class PProjectRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalProject.class,
                        Convention.NONE, PConvention.INSTANCE,
                        "PProjectRule")
                .withRuleFactory(PProjectRule::new);


        protected PProjectRule(Config config) {
            super(config);
        }

        public RelNode convert(RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;
            return new PProject(
                    project.getCluster(),
                    project.getTraitSet().replace(PConvention.INSTANCE),
                    convert(project.getInput(), project.getInput().getTraitSet()
                            .replace(PConvention.INSTANCE)),
                    project.getProjects(),
                    project.getRowType());
        }
    }


    private static class PFilterRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalFilter.class,
                        Convention.NONE, PConvention.INSTANCE,
                        "PFilterRule")
                .withRuleFactory(PFilterRule::new);


        protected PFilterRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            final LogicalFilter filter = (LogicalFilter) rel;
            return new PFilter(
                    rel.getCluster(),
                    rel.getTraitSet().replace(PConvention.INSTANCE),
                    convert(filter.getInput(), filter.getInput().getTraitSet().
                            replace(PConvention.INSTANCE)),
                    filter.getCondition());
        }
    }

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

    private static class PJoinRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalJoin.class, Convention.NONE,
                        PConvention.INSTANCE, "PJoinRule")
                .withRuleFactory(PJoinRule::new);

        protected PJoinRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode relNode) {
            /* Write your code here */
            LogicalJoin join = (LogicalJoin) relNode;
            RelNode left = convert(join.getInput(0), join.getInput(0).getTraitSet().replace(PConvention.INSTANCE));
            RelNode right = convert(join.getInput(1), join.getInput(1).getTraitSet().replace(PConvention.INSTANCE));
            return new PJoin(
                    join.getCluster(),
                    join.getTraitSet().replace(PConvention.INSTANCE),
                    left,
                    right,
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType()
            );
        }
    }

    private static class PAggregateRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalAggregate.class, Convention.NONE,
                        PConvention.INSTANCE, "PAggregateRule")
                .withRuleFactory(PAggregateRule::new);

        protected PAggregateRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode relNode) {
            LogicalAggregate aggregate = (LogicalAggregate) relNode;
            return new PAggregate(
                    aggregate.getCluster(),
                    aggregate.getTraitSet().replace(PConvention.INSTANCE),
                    aggregate.getHints(),
                    convert(aggregate.getInput(),aggregate.getInput().getTraitSet().replace(PConvention.INSTANCE)),
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    aggregate.getAggCallList());
        }
    }

    private static class PSortRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalSort.class, Convention.NONE,
                        PConvention.INSTANCE, "PSortRule")
                .withRuleFactory(PSortRule::new);

        protected PSortRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode relNode) {
            LogicalSort sort = (LogicalSort) relNode;
            return new PSort(
                    sort.getCluster(),
                    sort.getTraitSet().replace(PConvention.INSTANCE),
                    sort.getHints(),
                    convert(sort.getInput(), sort.getInput().getTraitSet().replace(PConvention.INSTANCE)),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch);
        }
    }
}
