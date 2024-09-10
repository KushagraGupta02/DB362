import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import index.bplusTree.BPlusTreeIndexFile;
import index.bplusTree.BlockNode;
import index.bplusTree.InternalNode;
import index.bplusTree.LeafNode;


public class BPlusTreeTestFinal {
    private int getRootId(BPlusTreeIndexFile<String> b_plus_tree) {
        List<BlockNode> blocks = b_plus_tree.return_blocks();
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id, BPlusTreeIndexFile<String> b_plus_tree){
        List<BlockNode> blocks = b_plus_tree.return_blocks();
        return isLeaf(blocks.get(id));
    }

    
    public void inorderUtil(ArrayList<Integer> inorder, int current, BPlusTreeIndexFile<String> b_plus_tree) {
        
        List<BlockNode> blocks = b_plus_tree.return_blocks();
        if(isLeaf(current, b_plus_tree)) {
            Object[] keys = ((LeafNode<String>) blocks.get(current)).getKeys();
            for (Object key : keys) {
                int intValue = Integer.parseInt((String) key); 
                inorder.add(intValue);
            }
        }
        else {
            int[] children = ((InternalNode<String>) blocks.get(current)).getChildren();
            
            int total = children.length;

            // All the children except the last
            for (int i = 0; i < total - 1; i++)
                inorderUtil(inorder, children[i], b_plus_tree);

            // Print the current node's data
            Object[] keys = ((InternalNode<String>) blocks.get(current)).getKeys();
            for (Object key : keys) {
                int intValue = Integer.parseInt((String) key); 
                inorder.add(intValue);
            }

            // Last child
            inorderUtil(inorder, children[total-1], b_plus_tree);
        }
    }
    public ArrayList<Integer> return_inorder(BPlusTreeIndexFile<String> b_plus_tree) {
        int root = getRootId(b_plus_tree);
        ArrayList<Integer> inorder = new ArrayList<>();
        int current = root;
        boolean done = false;

        inorderUtil(inorder, current, b_plus_tree);
        return inorder;
    }
    
    
    public void preorderUtil(ArrayList<Integer> preorder, int current, BPlusTreeIndexFile<String> b_plus_tree) {
        
        List<BlockNode> blocks = b_plus_tree.return_blocks();
        if(isLeaf(current, b_plus_tree)) {
            Object[] keys = ((LeafNode<String>) blocks.get(current)).getKeys();
            for (Object key : keys) {
                int intValue = Integer.parseInt((String) key); 
                preorder.add(intValue);
            }
        }
        else {

            // Print the current node's data
            Object[] keys = ((InternalNode<String>) blocks.get(current)).getKeys();
            for (Object key : keys) {
                int intValue = Integer.parseInt((String) key); 
                preorder.add(intValue);
            }

            
            int[] children = ((InternalNode<Integer>) blocks.get(current)).getChildren();
            
            
            int total = children.length;
            // All the children except the last
            for (int i = 0; i < total - 1; i++)
                preorderUtil(preorder, children[i], b_plus_tree);

            // Last child
            preorderUtil(preorder, children[total-1], b_plus_tree);
        }
    }
    public ArrayList<Integer> return_preorder(BPlusTreeIndexFile<String> b_plus_tree) {
        int root = getRootId(b_plus_tree);
        ArrayList<Integer> preorder = new ArrayList<>();
        int current = root;
        boolean done = false;

        preorderUtil(preorder, current, b_plus_tree);

        return preorder;
    }


    

    public void testUtilInorder(String table, String column_name, int order, int[] inserts, int[] expected_inorder) {

        try{
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(order, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }

            ArrayList<Integer> inord = return_inorder(b_plus_tree);
            System.out.println(inord.size()+" <==> "+expected_inorder.length);
            assert(inord.size() == expected_inorder.length);

            for(int i = 0; i < inord.size(); i++){
                assert(inord.get(i) == expected_inorder[i]);
            }

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
    }

    public void testUtilPreorder(String table, String column_name, int order, int[] inserts, int[] expected_preorder) {
        
        try{
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(order, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }

            ArrayList<Integer> preord = return_preorder(b_plus_tree);

            assert(preord.size() == expected_preorder.length);

            for(int i = 0; i < preord.size(); i++){
                assert(preord.get(i) == expected_preorder[i]);
            }

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
    }

    public void testUtilBFS(String table, String column_name, int order, int[] inserts, int[] expected_bfs) {

        try{
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(order, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }

            ArrayList<String> bfs = b_plus_tree.return_bfs();

            assert(bfs.size() == expected_bfs.length);

            for(int i = 0; i < bfs.size(); i++){
                assert(Integer.parseInt(bfs.get(i)) == expected_bfs[i]);
            }

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }
    }


 

    
    @Test
    public void test_tree_search_1() {

        try {

            // int[] expected_inorder = {1,2};
            // int[] expected_preorder = {1,2};
            // int[] expected_bfs = {1,2};
            int[] inserts={1,2,3,4,5,6,7};
    
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(3, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }
            
            // int first = BlockNode.counter;

            int id = b_plus_tree.search(""+4);
            List<BlockNode> blocks = b_plus_tree.return_blocks();
            BlockNode node = blocks.get(id);
            // int second = BlockNode.counter;
            // System.out.println("First: "+first+" Second: "+second);
            boolean found=false;
            if (node instanceof LeafNode) {
                LeafNode<String> lf = (LeafNode<String>) node;
                Object[] keys = lf.getKeys(); 
                for (Object key : keys) {
                    Integer intValue = Integer.parseInt((String) key); 
                    // System.out.println(intValue);
                    if(intValue == 4) {
                        found=true;
                        break;
                    } else {
                        System.out.println("No found: "+intValue+intValue.getClass());
                    }
                }
                assert(found);
            } else {
                // throw new IOException("Node is not instance of LeafNode");
                fail("Node is not instance of LeafNode");
            }

        } catch (Exception e) {
            System.out.println(e);
            System.out.println(e.getCause());
            fail("Exception thrown");
        }

        System.out.println("Test test_tree_search_1 passed :)");
    }

    @Test
    public void test_tree_search_2() {

        // try {

            // int[] expected_inorder = {1,2};
            // int[] expected_preorder = {1,2};
            // int[] expected_bfs = {1,2};
            int[] inserts={1,2,3,4,5,6,7};
    
            BPlusTreeIndexFile<String> b_plus_tree;
            b_plus_tree = new BPlusTreeIndexFile<>(3, String.class);

            for(int i=0; i<inserts.length; i++) {
                b_plus_tree.insert(""+inserts[i], inserts[i]);
            }
            
            System.out.println("Starting search");
            int first = BlockNode.counter;
            int id = b_plus_tree.search(""+4);
            int second = BlockNode.counter;
            List<BlockNode> blocks = b_plus_tree.return_blocks();
            BlockNode node = blocks.get(id);
            System.out.println("First: "+first+" Second: "+second);
            boolean found=false;
            if (node instanceof LeafNode) {
                LeafNode<String> lf = (LeafNode<String>) node;
                Object[] keys = lf.getKeys(); 
                for (Object key : keys) {
                    Integer intValue = Integer.parseInt((String) key); 
                    System.out.println(intValue+"(==)");
                    if(intValue == 4) {
                        found=true;
                        break;
                    } else {
                        System.out.println("No found: "+intValue+intValue.getClass());
                    }
                }
                assert(found);
                assert((second-first)<=6);
            } else {
                // throw new IOException("Node is not instance of LeafNode");
                fail("Node is not instance of LeafNode");
            }

        // } catch (Exception e) {
        //     System.out.println(e);
        //     System.out.println(e.getCause());
        //     fail("Exception thrown");
        // }

        System.out.println("Test test_tree_search_2 passed :)");
    }

}
