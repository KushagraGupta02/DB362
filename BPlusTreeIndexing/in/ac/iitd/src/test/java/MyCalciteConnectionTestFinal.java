import optimizer.convention.PConvention;
import optimizer.rules.PRules;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;


public class MyCalciteConnectionTestFinal {
    
    @Test 
    public void testSFW_1() {

        try{

            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            
            System.out.println("\nCreating index on actor_id...");
            calciteConnection.create_index("actor", "actor_id", 10);

            SqlNode sqlNode = calciteConnection.parseSql("select * from actor\n"
                                    + "where actor_id >= 100");
                                    
            System.out.println("\n[+] Parsed SQL: \n" + sqlNode);
            SqlNode validatedSqlNode = calciteConnection.validateSql(sqlNode);
            System.out.println("\n[+] Validated SQL: \n" + validatedSqlNode);
            RelNode relNode = calciteConnection.convertSql(validatedSqlNode);
            System.out.println("\n[+] Converted SQL: \n" + relNode);

            RuleSet rules = RuleSets.ofList(
                PRules.PCustomRule.INSTANCE
            );

            RelNode phyRelNode = calciteConnection.logicalToPhysical(
                    relNode,
                    relNode.getTraitSet().plus(PConvention.INSTANCE),
                    rules
            );

            System.out.println("\n[+] Physical SQL: \n" + phyRelNode);

            System.out.println("\n[+] Evaluating physical SQL");
            List<Object []> result = calciteConnection.evaluate(phyRelNode);

            // Uncomment this to check the records returned by IndexScan
            
            assert (result.size() == 101);

            List <Integer> actor_ids = new ArrayList<>();

            for (Object [] row : result) {
                assert (row.length == 4);
                assert (row[0] instanceof Integer);
                assert ((Integer)row[0] >= 100);
                actor_ids.add((Integer)row[0]);
            }

            // sort the actor_ids
            actor_ids.sort(null);

            // result actor_ids should be from 100 to 200
            for (int i = 0; i < actor_ids.size(); i++) {
                assert (actor_ids.get(i).equals(100 + i));
            }

            calciteConnection.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("\nTest 1 SFW passed :)\n");
        return;
    }

    @Test 
    public void testSFW_2() {

        try{

            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            
            System.out.println("\nCreating index on customer_id...");
            calciteConnection.create_index("customer", "customer_id", 10);

            SqlNode sqlNode = calciteConnection.parseSql("select * from customer\n"
                                    + "where customer_id <= 50");
                                    
            System.out.println("\n[+] Parsed SQL: \n" + sqlNode);
            SqlNode validatedSqlNode = calciteConnection.validateSql(sqlNode);
            System.out.println("\n[+] Validated SQL: \n" + validatedSqlNode);
            RelNode relNode = calciteConnection.convertSql(validatedSqlNode);
            System.out.println("\n[+] Converted SQL: \n" + relNode);

            RuleSet rules = RuleSets.ofList(
                PRules.PCustomRule.INSTANCE
            );

            RelNode phyRelNode = calciteConnection.logicalToPhysical(
                    relNode,
                    relNode.getTraitSet().plus(PConvention.INSTANCE),
                    rules
            );

            System.out.println("\n[+] Physical SQL: \n" + phyRelNode);

            System.out.println("\n[+] Evaluating physical SQL");
            List<Object []> result = calciteConnection.evaluate(phyRelNode);

            // Uncomment this to check the records returned by IndexScan
            
            assert (result.size() == 50);
            
            List <Integer> customer_ids = new ArrayList<>();
            System.out.println("=="+result.size());
            for (Object [] row : result) {
                // System.out.println(""+row.length+"=="+result.size());
                assert (row.length == 10);
                assert (row[0] instanceof Integer);
                assert ((Integer)row[0] <= 50);
                customer_ids.add((Integer)row[0]);
            }
// assert(false);
            calciteConnection.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("\nTest 2 SFW passed :)\n");
        return;
    }

    @Test 
    public void testSFW_3() {

        try{

            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            
            System.out.println("\nCreating index on actor_id...");
            calciteConnection.create_index("actor", "actor_id", 10);

            SqlNode sqlNode = calciteConnection.parseSql("select * from actor\n"
                                    + "where actor_id > 100");
                                    
            System.out.println("\n[+] Parsed SQL: \n" + sqlNode);
            SqlNode validatedSqlNode = calciteConnection.validateSql(sqlNode);
            System.out.println("\n[+] Validated SQL: \n" + validatedSqlNode);
            RelNode relNode = calciteConnection.convertSql(validatedSqlNode);
            System.out.println("\n[+] Converted SQL: \n" + relNode);

            RuleSet rules = RuleSets.ofList(
                PRules.PCustomRule.INSTANCE
            );

            RelNode phyRelNode = calciteConnection.logicalToPhysical(
                    relNode,
                    relNode.getTraitSet().plus(PConvention.INSTANCE),
                    rules
            );

            System.out.println("\n[+] Physical SQL: \n" + phyRelNode);

            System.out.println("\n[+] Evaluating physical SQL");
            List<Object []> result = calciteConnection.evaluate(phyRelNode);

            // Uncomment this to check the records returned by IndexScan
            
            assert (result.size() == 100);

            List <Integer> actor_ids = new ArrayList<>();

            for (Object [] row : result) {
                assert (row.length == 4);
                assert (row[0] instanceof Integer);
                assert ((Integer)row[0] > 100);
                actor_ids.add((Integer)row[0]);
            }

            calciteConnection.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("\nTest 3 SFW passed :)\n");
        return;
    }

    @Test 
    public void testSFW_4() {

        try{

            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            
            System.out.println("\nCreating index on customer_id...");
            calciteConnection.create_index("customer", "customer_id", 10);

            SqlNode sqlNode = calciteConnection.parseSql("select * from customer\n"
                                    + "where customer_id < 50");
                                    
            System.out.println("\n[+] Parsed SQL: \n" + sqlNode);
            SqlNode validatedSqlNode = calciteConnection.validateSql(sqlNode);
            System.out.println("\n[+] Validated SQL: \n" + validatedSqlNode);
            RelNode relNode = calciteConnection.convertSql(validatedSqlNode);
            System.out.println("\n[+] Converted SQL: \n" + relNode);

            RuleSet rules = RuleSets.ofList(
                PRules.PCustomRule.INSTANCE
            );

            RelNode phyRelNode = calciteConnection.logicalToPhysical(
                    relNode,
                    relNode.getTraitSet().plus(PConvention.INSTANCE),
                    rules
            );

            System.out.println("\n[+] Physical SQL: \n" + phyRelNode);

            System.out.println("\n[+] Evaluating physical SQL");
            List<Object []> result = calciteConnection.evaluate(phyRelNode);

            // Uncomment this to check the records returned by IndexScan
            
            assert (result.size() == 49);
            
            List <Integer> customer_ids = new ArrayList<>();
            System.out.println("=="+result.size());
            for (Object [] row : result) {
                assert (row.length == 10);
                assert (row[0] instanceof Integer);
                assert ((Integer)row[0] < 50);
                customer_ids.add((Integer)row[0]);
            }
            calciteConnection.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("\nTest 4 SFW passed :)\n");
        return;
    }

    @Test 
    public void testSFW_5() {

        try{

            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            
            System.out.println("\nCreating index on customer_id...");
            calciteConnection.create_index("customer", "customer_id", 10);

            SqlNode sqlNode = calciteConnection.parseSql("select * from customer\n"
                                    + "where customer_id = 50");
                                    
            System.out.println("\n[+] Parsed SQL: \n" + sqlNode);
            SqlNode validatedSqlNode = calciteConnection.validateSql(sqlNode);
            System.out.println("\n[+] Validated SQL: \n" + validatedSqlNode);
            RelNode relNode = calciteConnection.convertSql(validatedSqlNode);
            System.out.println("\n[+] Converted SQL: \n" + relNode);

            RuleSet rules = RuleSets.ofList(
                PRules.PCustomRule.INSTANCE
            );

            RelNode phyRelNode = calciteConnection.logicalToPhysical(
                    relNode,
                    relNode.getTraitSet().plus(PConvention.INSTANCE),
                    rules
            );

            System.out.println("\n[+] Physical SQL: \n" + phyRelNode);

            System.out.println("\n[+] Evaluating physical SQL");
            List<Object []> result = calciteConnection.evaluate(phyRelNode);

            // Uncomment this to check the records returned by IndexScan
            
            assert (result.size() == 1);
            
            List <Integer> customer_ids = new ArrayList<>();
            System.out.println("=="+result.size());
            for (Object [] row : result) {
                assert (row.length == 10);
                assert (row[0] instanceof Integer);
                assert ((Integer)row[0] == 50);
                customer_ids.add((Integer)row[0]);
            }
            calciteConnection.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("\nTest 5 SFW passed :)\n");
        return;
    }

}
