import convention.PConvention;

import org.apache.calcite.rel.RelNode;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.List;

public class PrimaryTestCases {

    public RelNode createRelNode(String query, MyCalciteConnection calciteConnection) {
        try{
            RelNode relNode = calciteConnection.convertSql(calciteConnection.validateSql(calciteConnection.parseSql(query)));
            return relNode;
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    public List<Object []> eval(RelNode relNode, MyCalciteConnection calciteConnection) {
        try{

            RelNode phyRelNode = calciteConnection.logicalToPhysical(
                relNode,
                relNode.getTraitSet().plus(PConvention.INSTANCE)
            );

            // You should check here that the physical relNode is a PProjectFilter instance
            System.out.println("[+] Physical RelNode:\n" + phyRelNode.explain());

            List<Object []> result = calciteConnection.executeQuery(phyRelNode);
            return result;
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    
    @Test 
    public void testSFW() {
        try{
            MyCalciteConnection calciteConnection = new MyCalciteConnection();
            String query = "select first_name from actor where actor_id > 100 and actor_id < 150";
            
            RelNode relNode = createRelNode(query, calciteConnection);
            System.out.println("[+] Logical RelNode:\n" + relNode.explain());
            List<Object []> result = eval(relNode, calciteConnection);

            assert(result.size() == 49);
            for(Object [] row : result){
                assert(row.length == 1);
            }
            // sort the result
            result.sort((a, b) -> ((String)a[0]).compareTo((String)b[0]));

            String [] expected = new String [] {
                "Adam",
                "Albert",
                "Albert",
                "Angela",
                "Cameron",
                "Cate",
                "Cate",
                "Cuba",
                "Dan",
                "Daryl",
                "Ed",
                "Emily",
                "Ewan",
                "Fay",
                "Frances",
                "Gene",
                "Gina",
                "Greta",
                "Groucho",
                "Harrison",
                "Jada",
                "Jane",
                "Julianne",
                "Kevin",
                "Kim",
                "Liza",
                "Lucille",
                "Matthew",
                "Morgan",
                "Morgan",
                "Morgan",
                "Penelope",
                "Penelope",
                "Renee",
                "Richard",
                "Rita",
                "River",
                "Russell",
                "Russell",
                "Salma",
                "Scarlett",
                "Sidney",
                "Susan",
                "Susan",
                "Sylvester",
                "Walter",
                "Warren",
                "Warren",
                "Whoopi"
            };

            for(int i = 0; i < result.size(); i++){
                assert(result.get(i)[0].equals(expected[i]));
            }

            // Tip: You can use the following code to print the result and debug

            // if(result == null) {
            //     System.out.println("[-] No result found");
            // }
            // else{
            //     System.out.println("[+] Final Output : ");
            //     for (Object [] row : result) {
            //         for (Object col : row) {
            //             System.out.print(col + " ");
            //         }
            //         System.out.println();
            //     }
            // }

            calciteConnection.close();
        }
        catch(Exception e){
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
        System.out.println("Test passed :)");
        return;
    }

}