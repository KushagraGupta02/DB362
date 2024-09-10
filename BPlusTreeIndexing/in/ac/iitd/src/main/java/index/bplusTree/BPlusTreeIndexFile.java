package index.bplusTree;

import storage.AbstractFile;

import java.nio.ByteBuffer;
import java.util.*;

/*
 * Tree is a collection of BlockNodes
 * The first BlockNode is the metadata block - stores the order and the block_id of the root node

 * The total number of keys in all leaf nodes is the total number of records in the records file.
 */

public class BPlusTreeIndexFile<T> extends AbstractFile<BlockNode> {

    Class<T> typeClass;
    public class Pair<U, V> {

        private U first;
        private V second;

        public Pair(U first, V second) {

            this.first = first;
            this.second = second;
        }
        public U getFirst() {
            return first;
        }
        public V getSecond() {
            return second;
        }
    }
    // Constructor - creates the metadata block and the root node
    public BPlusTreeIndexFile(int order, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;
        BlockNode node = new BlockNode(); // the metadata block
        LeafNode<T> root = new LeafNode<>(typeClass);

        // 1st 2 bytes in metadata block is order
        byte[] orderBytes = new byte[2];
        orderBytes[0] = (byte) (order >> 8);
        orderBytes[1] = (byte) order;
        node.write_data(0, orderBytes);

        // next 2 bytes are for root_node_id, here 1
        byte[] rootNodeIdBytes = new byte[2];
        rootNodeIdBytes[0] = 0;
        rootNodeIdBytes[1] = 1;
        node.write_data(2, rootNodeIdBytes);

        // push these nodes to the blocks list
        blocks.add(node);
        blocks.add(root);
    }

    private boolean isFull(int id){
        // 0th block is metadata block
        assert(id > 0);
        return blocks.get(id).getNumKeys() == getOrder() - 1;
    }

    private int getRootId() {
        BlockNode node = blocks.get(0);
        byte[] rootBlockIdBytes = node.get_data(2, 2);
        return (rootBlockIdBytes[0] << 8) | (rootBlockIdBytes[1] & 0xFF);
    }

    private void setRootId(int id){
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) (id >> 8);
        numKeysBytes[1] = (byte) id;
        blocks.get(0).write_data(2, numKeysBytes);
    }
    public int getOrder() {
        BlockNode node = blocks.get(0);
        byte[] orderBytes = node.get_data(0, 2);
        return (orderBytes[0] << 8) | (orderBytes[1] & 0xFF);
    }

    public byte[] convertTToBytes(T value, Class<T> typeClass) {
        if (typeClass == Integer.class) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt((Integer)value);
            return buffer.array();
        } else if (typeClass == Long.class) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong((Long)value);
            return buffer.array();
        } else if (typeClass == Double.class) {
            ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
            buffer.putDouble((Double)value);
            return buffer.array();
        } else if (typeClass == Boolean.class) {
            byte[] result = new byte[1];
            result[0] = ((Boolean)value) ? (byte)1 : (byte)0;
            return result;
        } else if (typeClass == String.class) {
            return ((String)value).getBytes();
        } else {
            return null; // Unsupported type
        }
    }


    // Might be useful for you - will not be evaluated
    public T convertBytesToT(byte[] bytes, Class<T> typeClass) {
        // Generic type T, int , float, double bool, B+ tree, byte array to type T
        /* Write your code here */
        if (typeClass == Integer.class) {
            return typeClass.cast(ByteBuffer.wrap(bytes).getInt());
        } else if (typeClass == Long.class) {
            return typeClass.cast(ByteBuffer.wrap(bytes).getLong());
        } else if (typeClass == Double.class) {
            return typeClass.cast(ByteBuffer.wrap(bytes).getDouble());
        } else if (typeClass == Boolean.class) {
            return typeClass.cast(bytes[0] != 0);
        } else if (typeClass == String.class) {
            return typeClass.cast(new String(bytes));
        } else {
            return null;
        }
    }

    private boolean isLeaf(BlockNode node){
        return node instanceof LeafNode;
    }

    private boolean isLeaf(int id){
        return isLeaf(blocks.get(id));
    }

    // procedure SplitLeafNode(leafNode, key, value):
    //    newLeafNode = CreateNewLeafNode()
    //    MoveHalfOfKeysToNewLeafNode(leafNode, newLeafNode)
    //    if key >= last key in leafNode:
    //        InsertKeyInLeafNode(newLeafNode, key, value)
    //    else:
    //        InsertKeyInLeafNode(leafNode, key, value)
    //    newKey = FirstKeyIn(newLeafNode)
    //    LinkNodes(leafNode, newLeafNode)
    //    return newLeafNode, newKey

    // procedure AdjustParentAfterLeafNodeSplit(leafNode, newKey):
    //    parent = leafNode.parent
    //    if parent is None:
    //        root = CreateNewInternalNode()
    //        tree.root = root
    //        parent = root
    //        leafNode.parent = root
    //    InsertKeyInInternalNode(parent, newKey, leafNode)

    // procedure AdjustParents(tree, node):
    //    if node is tree.root:
    //        return
    //    parent = node.parent
    //    if node needs splitting:
    //        sibling, newKey = SplitInternalNode(node)
    //        AdjustParentAfterInternalNodeSplit(parent, sibling, newKey)
    //    AdjustParents(tree, parent)

    //  -- leafNode, newKey = SplitLeafNode(leafNode, key, value)
    //  -- AdjustParentAfterLeafNodeSplit(leafNode, newKey)
    //  -- AdjustParents(tree, leafNode)

    public void splitLeafNode(int leaf_block_id, int parent_block_id, T key, int block_id, Stack<Integer> parents_block_id_list) {
        LeafNode<T>  leaf  = (LeafNode<T>) blocks.get(leaf_block_id);
        int numKeys = leaf.getNumKeys();
        T[] keys = leaf.getKeys();
        int[] blocks_temp = leaf.getBlockIds();

        T[] newkeys = (T[]) new Object[numKeys+1];
        int[] newblocks = new int[numKeys+1];

        // insert key in the correct position
        int insertIndex = 0;
        for (insertIndex = 0; insertIndex < numKeys; insertIndex++) {
            if (compareTo(typeClass,key,keys[insertIndex]) > 0) {
                newkeys[insertIndex] = keys[insertIndex];
                newblocks[insertIndex] = blocks_temp[insertIndex];
            } else {
                break;
            }
        }
        newkeys[insertIndex] = key;
        newblocks[insertIndex] = block_id;
        for (int i = insertIndex; i < numKeys; i++) {
            newkeys[i+1] = keys[i];
            newblocks[i+1] = blocks_temp[i];
        }

        int order = numKeys+1;
        int splitIndex = order/2;
        T my_key = newkeys[splitIndex];
        T last_key_in_old = newkeys[splitIndex-1];

        leaf.setNumKeys(splitIndex);
        int off = 8;
        for (int i=0; i<splitIndex; i++) {
            T key_temp = newkeys[i];
            int block_id_temp = newblocks[i];
            byte [] blockIdBytes = new byte[2];
            byte[] keyBytes = convertTToBytes(key_temp, typeClass);
            blockIdBytes[0] = (byte) ((block_id_temp >> 8) & 0xFF);
            blockIdBytes[1] = (byte) (block_id_temp & 0xFF);
            leaf.write_data(off, blockIdBytes);
            byte[] key_length_bytes = new byte[2];
            key_length_bytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
            key_length_bytes[1] = (byte) (keyBytes.length & 0xFF);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8) | (key_length_bytes[1] & 0xFF);
            leaf.write_data(off+2, key_length_bytes);
            leaf.write_data(off+4, keyBytes);
            off += 2 + 2 + key_length;
        }
        leaf.setFreeOffset(off);

        LeafNode<T> newLeaf = new LeafNode<T>(typeClass);
        newLeaf.setNumKeys(order - order/2);
        int off_new = 8;
        for (int i = splitIndex; i < order; i++) {
            T key_temp = newkeys[i];
            int block_id_temp = newblocks[i];
            byte [] blockIdBytes = new byte[2];
            byte[] keyBytes = convertTToBytes(key_temp, typeClass);
            blockIdBytes[0] = (byte) ((block_id_temp >> 8) & 0xFF);
            blockIdBytes[1] = (byte) (block_id_temp & 0xFF);
            newLeaf.write_data(off_new, blockIdBytes);
            byte[] key_length_bytes = new byte[2];
            key_length_bytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
            key_length_bytes[1] = (byte) (keyBytes.length & 0xFF);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8) | (key_length_bytes[1] & 0xFF);
            newLeaf.write_data(off_new+2, key_length_bytes);
            newLeaf.write_data(off_new+4, keyBytes);
            off_new += 2 + 2 + key_length;
        }
        newLeaf.setFreeOffset(off_new);
        int tempset = leaf.getNext();
        blocks.add(newLeaf);
        int temp_key_block_id = blocks.size()-1;
        leaf.setNext(temp_key_block_id);
        newLeaf.setPrev(leaf_block_id);
        newLeaf.setNext(tempset);
        if (tempset!=0){
            LeafNode<T> temp = (LeafNode<T>) blocks.get(tempset);
            temp.setPrev(temp_key_block_id);
        }
        int temp_parent_block_id = parent_block_id;
        T temp_inner_key = my_key;
        int current = leaf_block_id;
        //
        while (temp_parent_block_id!=-1){
            if (!isFull(temp_parent_block_id)) {
                InternalNode<T> parent = (InternalNode<T>) blocks.get(temp_parent_block_id);
                parent.insert(temp_inner_key,temp_key_block_id);
                break;
            }
            else{
                parents_block_id_list.pop();
                Pair l = splitInnerNode(temp_parent_block_id, temp_inner_key, temp_key_block_id);
                T new_my_key = (T) l.getSecond();
                int new_sibling = (int) l.getFirst();
                InternalNode<T> parent = (InternalNode<T>) blocks.get(temp_parent_block_id);
                current = temp_parent_block_id;
                temp_key_block_id = new_sibling;
                temp_parent_block_id = parents_block_id_list.peek();
                temp_inner_key= new_my_key;
            }

        }
        if (temp_parent_block_id == -1){
            InternalNode<T> newRoot = new InternalNode<T>(temp_inner_key,current, temp_key_block_id, typeClass);
            blocks.add(newRoot);
            setRootId(blocks.size()-1);
            int tempest = newRoot.getNumKeys();
            return;
        }
        return;
    }

    // will be evaluated
    public Pair<Integer, T> splitInnerNode(int block_id, T key, int right_block_id) {


        InternalNode<T>  inner  = (InternalNode<T>) blocks.get(block_id);
        int numKeys = inner.getNumKeys();
        T[] keys = inner.getKeys();
        int[] blocks_temp = inner.getChildren();

        T[] newkeys = (T[]) new Object[numKeys+1];
        int[] newchildren = new int[numKeys+2];

        int insertIndex = 0;
        for (insertIndex = 0; insertIndex < numKeys; insertIndex++) {
            if (compareTo(typeClass,key,keys[insertIndex]) >= 0) {
                newkeys[insertIndex] = keys[insertIndex];
                newchildren[insertIndex] = blocks_temp[insertIndex];
            } else {
                break;
            }
        }
        newkeys[insertIndex] = key;
        newchildren[insertIndex] = blocks_temp[insertIndex];
        newchildren[insertIndex+1] = right_block_id;
        for (int i = insertIndex; i < numKeys; i++) {
            newkeys[i+1] = keys[i];
            newchildren[i+2] = blocks_temp[i+1];
        }

        int order = numKeys+1;
        int splitIndex = (order)/2; // 3 keys then 4 keys, so splitIndex = 2, 4 keys then index = 2 again

        T my_key = newkeys[splitIndex];
        T last_key_in_old = newkeys[splitIndex-1];

        inner.setNumKeys(splitIndex);
        int off = 6;
        int child_id_temp_2 = newchildren[0];
        byte [] childIdBytes_2 = new byte[2];
        childIdBytes_2[0] = (byte) ((child_id_temp_2 >> 8) & 0xFF);
        childIdBytes_2[1] = (byte) (child_id_temp_2 & 0xFF);
        inner.write_data(4, childIdBytes_2);
        for (int i=0; i<splitIndex; i++) {
            T key_temp = newkeys[i];
            int child_id_temp = newchildren[i+1];
            byte [] childIdBytes = new byte[2];
            byte[] keyBytes = convertTToBytes(key_temp, typeClass);
            childIdBytes[0] = (byte) ((child_id_temp >> 8) & 0xFF);
            childIdBytes[1] = (byte) (child_id_temp & 0xFF);
            byte[] key_length_bytes = new byte[2];
            key_length_bytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
            key_length_bytes[1] = (byte) (keyBytes.length & 0xFF);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8) | (key_length_bytes[1] & 0xFF);
            inner.write_data(off, key_length_bytes);
            inner.write_data(off+2, keyBytes);
            inner.write_data(off+2+key_length, childIdBytes);
            off += 2 + 2 + key_length;
        }
//        inner.setFreeOffset(off);
//
//        byte [] my_key_bytes = convertTToBytes(my_key,typeClass);
//        int length = my_key_bytes.length;
//        byte[] left_first_pointer = inner.get_data(off+2+length, 2);
//        int left_length = (left_first_pointer[0] << 8) | (left_first_pointer[1] & 0xFF);


        int off_new = 6;
        int child_id_temp_sec_2 = newchildren[splitIndex+1];

        InternalNode<T> newInner = new InternalNode<T>(child_id_temp_sec_2,typeClass);
        newInner.setNumKeys(order - order/2-1);
//        newInner.write_data(4, childIdBytes_sec_2);
        for (int i = splitIndex+1; i < order; i++) {
            T key_temp = newkeys[i];
            int child_id_temp = newchildren[i+1];
            byte [] blockIdBytes = new byte[2];
            byte [] childIdBytes = new byte[2];
            byte[] keyBytes = convertTToBytes(key_temp, typeClass);
            childIdBytes[0] = (byte) ((child_id_temp >> 8) & 0xFF);
            childIdBytes[1] = (byte) (child_id_temp & 0xFF);
            byte[] key_length_bytes = new byte[2];
            key_length_bytes[0] = (byte) ((keyBytes.length >> 8) & 0xFF);
            key_length_bytes[1] = (byte) (keyBytes.length & 0xFF);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8) | (key_length_bytes[1] & 0xFF);
            newInner.write_data(off_new, key_length_bytes);
            newInner.write_data(off_new+2, keyBytes);
            newInner.write_data(off_new+2+key_length, childIdBytes);
            off_new += 2 + 2 + key_length;;
        }
        newInner.setFreeOffset(off_new);
        blocks.add(newInner);
        return new Pair(blocks.size()-1,my_key);
    }
    public void insert(T key, int block_id) {

        /* Write your code here */
        int currentNode = getRootId();
        int parentNode = -1;
        Stack <Integer> parents_block_id_list = new Stack<>();
        while (!(isLeaf(currentNode))) {
            parents_block_id_list.push(parentNode);
            parentNode = currentNode;
            InternalNode<T> internalNode = (InternalNode<T>) blocks.get(currentNode);
            int i = 0;
            while (i < internalNode.getNumKeys() && compareTo(typeClass,key, internalNode.getKeys()[i]) >= 0) {
                i++;
            }
            if (i == 0) {
                currentNode = internalNode.getChildren()[0];
            } else {
                currentNode = (internalNode.getChildren()[i]);
            }
        }
        LeafNode<T> leafNode = (LeafNode<T>) blocks.get(currentNode);
        parents_block_id_list.push(parentNode);
        if (isFull(currentNode)) {
            // split the leaf node
            splitLeafNode(currentNode,parentNode, key, block_id, parents_block_id_list);
        }
        else{
            leafNode.insert(key,block_id);
        }
        return;
    }

    public int compareTo(Class<T> typeClass, T first, T second) {
        // Generic type T, int , float, double bool, B+ tree, byte array to type T
        /* Write your code here */
        if (typeClass == Integer.class) {
            return Integer.compare((Integer) first, (Integer) second);
        } else if (typeClass == Float.class) {
            return Float.compare((Float) first, (Float) second);
        } else if (typeClass == Double.class) {
            return Double.compare((Double) first, (Double) second);
        } else if (typeClass == Boolean.class) {
            // For boolean values, you might want to define custom comparison logic
            // For example, return 1 if first is true and second is false, and -1 otherwise
            boolean a = (Boolean) first;
            boolean b = (Boolean) second;
            if (a == b) {
                return 0;
            } else if (a) {
                return 1;
            } else {
                return -1;
            }
        } else if (typeClass == String.class) {
            return ((String) first).compareTo((String) second);}
        return 0;
    }
    // will be evaluated
    // returns the block_id of the leftmost leaf node containing the key
    public int search(T key) {
        /* Write your code here */
        int currentNode = getRootId();
        while (!(isLeaf(currentNode))) {
            InternalNode<T> internalNode = (InternalNode<T>) blocks.get(currentNode);
            int i = 0;
            while (i < (internalNode.get_data(0,2) [0] << 8 | internalNode.get_data(0,2)[1] & 0xFF) && compareTo(typeClass,key, internalNode.getKeys()[i]) > 0) {
                i++;
            }
            currentNode = (internalNode.getChildren()[i]);
        }
        LeafNode<T> leafNode = (LeafNode<T>) blocks.get(currentNode);

        if (leafNode.search(key) != -1){
            Object keys[] = leafNode.getKeys();
            if (compareTo(typeClass,key, (T) keys[keys.length-1]) > 0){
                if (leafNode.getNext() != 0){
                    return leafNode.getNext();
                }
                else {
                    return -1;
                }
            }
            return currentNode;
        }
        else{
            int prev = currentNode;
            while (leafNode.search(key) != -1){
                prev = currentNode;
                currentNode = leafNode.getPrev();
                if (currentNode == 0){
                    return prev;
                }
                leafNode = (LeafNode<T>) blocks.get(currentNode);
            }
            return prev+1;
        }
    }

    // returns true if the key was found and deleted, false otherwise
    // (Optional for Assignment 3)
    public boolean delete(T key) {

        /* Write your code here */
        return false;
    }

    // DO NOT CHANGE THIS - will be used for evaluation
    public void print_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                ((LeafNode<T>) blocks.get(id)).print();
            }
            else {
                ((InternalNode<T>) blocks.get(id)).print();
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return;
    }
    public void printBFS_new() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        queue.add(root);
        int nodesAtCurrentLevel = 1;
        int nodesAtNextLevel = 0;

        while (!queue.isEmpty()) {
            int id = queue.remove();
            if (isLeaf(id)) {
                LeafNode<T> leafNode = (LeafNode<T>) blocks.get(id);
                System.out.print("l: " + id + " |");
                for (int i = 0; i< leafNode.getNumKeys(); i++){
                    System.out.print(leafNode.getKeys()[i] + " ");
                    System.out.print("[" + leafNode.getBlockIds()[i] + "]");
                    System.out.print(" ");
                }
                System.out.print(" | "); // Separator between leaf nodes
            } else {
                InternalNode<T> internalNode = (InternalNode<T>) blocks.get(id);
                System.out.print("b: " + id + " |");
                System.out.print("[" +internalNode.getChildren()[0] + "] ");
                for (int i = 0; i< internalNode.getNumKeys(); i++){
                    System.out.print(internalNode.getKeys()[i]);
                    System.out.print("[" + internalNode.getChildren()[i+1] + "] ");
                }
                System.out.print(" | "); // Separator between internal nodes
                int[] children = internalNode.getChildren();
                for (int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                    nodesAtNextLevel++;
                }
            }

            nodesAtCurrentLevel--;
            if (nodesAtCurrentLevel == 0) {
                System.out.println(); // Print new line
                nodesAtCurrentLevel = nodesAtNextLevel;
                nodesAtNextLevel = 0;
            }
        }
    }
    // DO NOT CHANGE THIS - will be used for evaluation
    public ArrayList<T> return_bfs() {
        int root = getRootId();
        Queue<Integer> queue = new LinkedList<>();
        ArrayList<T> bfs = new ArrayList<>();
        queue.add(root);
        while(!queue.isEmpty()) {
            int id = queue.remove();
            if(isLeaf(id)) {
                T[] keys = ((LeafNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
            }
            else {
                T[] keys = ((InternalNode<T>) blocks.get(id)).getKeys();
                for(int i = 0; i < keys.length; i++) {
                    bfs.add((T) keys[i]);
                }
                int[] children = ((InternalNode<T>) blocks.get(id)).getChildren();
                for(int i = 0; i < children.length; i++) {
                    queue.add(children[i]);
                }
            }
        }
        return bfs;
    }

    public void print() {
        print_bfs();
        return;
    }

}