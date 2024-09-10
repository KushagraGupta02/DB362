package index.bplusTree;

import java.util.Arrays;

/*
 * Internal Node - num Keys | ptr to next free offset | P_1 | len(K_1) | K_1 | P_2 | len(K_2) | K_2 | ... | P_n
 * Only write code where specified

 * Remember that each Node is a block in the Index file, thus, P_i is the block_id of the child node
 */
public class InternalNode<T> extends BlockNode implements TreeNode<T> {

    // Class of the key
    Class<T> typeClass;

    // Constructor - expects the key, left and right child ids
    public InternalNode(T key, int left_child_id, int right_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);

        // also calls the insert method
        this.insert(key, right_child_id);
        return;
    }

    public InternalNode(int left_child_id, Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = 0;
        numKeysBytes[1] = 0;

        this.write_data(0, numKeysBytes);

        byte[] child_1 = new byte[2];
        child_1[0] = (byte) ((left_child_id >> 8) & 0xFF);
        child_1[1] = (byte) (left_child_id & 0xFF);

        this.write_data(4, child_1);

        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 6;

        this.write_data(2, nextFreeOffsetBytes);
        return;
    }

    public void setNumKeys(int numKeys) {
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) ((numKeys >> 8) & 0xFF); // Most significant byte
        numKeysBytes[1] = (byte) (numKeys & 0xFF);       // Least significant byte
        this.write_data(0, numKeysBytes);       // Write the bytes to the node
    }

    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        /* Write your code here */
        int off = 4;
        for (int i=0; i<numKeys; i++) {
            byte[] key_length_bytes = this.get_data(off +2, 2);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8) | (key_length_bytes[1] & 0xFF);
            byte[] key_bytes = this.get_data(off + 4, key_length);
            keys[i] = (T) convertBytesToT(key_bytes, typeClass);
            off += 2 + 2 + key_length;
        }


        return keys;
    }

    // can be used as helper function - won't be evaluated

    public int getFreeOffset() {
        byte[] numKeysBytes = this.get_data(2, 2);
        return ((numKeysBytes[0] & 0xFF) << 8) | (numKeysBytes[1] & 0xFF);
    }
    public void setFreeOffset(int offset) {
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) ((offset >> 8) & 0xFF); // Most significant byte
        numKeysBytes[1] = (byte) (offset & 0xFF);       // Least significant byte
        this.write_data(2, numKeysBytes);       // Write the bytes to the node
    }

    //procedure InsertKeyInInternalNode(node, key, child):
    //    Insert key-child pair into node
    @Override
    public void insert(T key, int right_block_id) {
        /* Write your code here */
        T[] keys = this.getKeys();
        int numKeys = this.getNumKeys();
        int insertIndex = 0;
        while (insertIndex < numKeys) {
            if (compareTo(typeClass,key,keys[insertIndex]) >= 0) {
                insertIndex++;
            }
            else {
                break;
            }
        }
        int empty = this.getFreeOffset();
        int off = 6;
        for (int i=0; i<insertIndex; i++) {
            byte[] key_length_bytes = this.get_data(off, 2);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8) | (key_length_bytes[1] & 0xFF);
            off += 2 + 2 + key_length;
        }
        byte[] remaining_data;
        remaining_data = this.get_data(off,empty-off );
        byte[] keyBytes = convertTToBytes(key, typeClass);
        byte[] blockIdBytes = new byte[2];
        blockIdBytes[0] = (byte) ((right_block_id >> 8) & 0xFF);
        blockIdBytes[1] = (byte) (right_block_id & 0xFF);
        int new_key_length = keyBytes.length;
        byte[] new_key_length_bytes = new byte[2];
        new_key_length_bytes[0] = (byte) ((new_key_length >> 8) & 0xFF);
        new_key_length_bytes[1] = (byte) (new_key_length & 0xFF);

        this.write_data(off, new_key_length_bytes);
        this.write_data(off+2, keyBytes);
        this.write_data(off+2+new_key_length, blockIdBytes);

        this.write_data(off+4+new_key_length, remaining_data);
        setFreeOffset(empty+4+new_key_length);
        numKeys++;
        this.setNumKeys(numKeys);
        return;

    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {
        /* Write your code here */
        return -1;
    }

    // should return the block_ids of the children - will be evaluated
    public int[] getChildren() {

        byte[] numKeysBytes = this.get_data(0, 2);
        int numKeys = ((numKeysBytes[0] & 0xFF) << 8) | (numKeysBytes[1] & 0xFF);

        int[] children = new int[numKeys + 1];

        /* Write your code here */
        int off = 4;
        /* Write your code here */
        for (int i=0; i<numKeys+1; i++) {
            byte[] child_bytes = this.get_data(off, 2);
            children[i] = ((child_bytes[0] & 0xFF) << 8) | (child_bytes[1] & 0xFF);
            byte[] key_length_bytes = this.get_data(off +2, 2);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8) | (key_length_bytes[1] & 0xFF);
            off += 2 + 2 + key_length;
        }
        return children;

    }

}
