package index.bplusTree;

import java.util.Arrays;

/*
 * A LeafNode contains keys and block ids.
 * Looks Like -
 * # entries | prev leafnode | next leafnode | ptr to next free offset | blockid_1 | len(key_1) | key_1 ...
 *
 * Note: Only write code where specified!
 */
public class LeafNode<T> extends BlockNode implements TreeNode<T>{

    Class<T> typeClass;

    public LeafNode(Class<T> typeClass) {

        super();
        this.typeClass = typeClass;

        // set numEntries to 0
        byte[] numEntriesBytes = new byte[2];
        numEntriesBytes[0] = 0;
        numEntriesBytes[1] = 0;
        this.write_data(0, numEntriesBytes);

        // set ptr to next free offset to 8
        byte[] nextFreeOffsetBytes = new byte[2];
        nextFreeOffsetBytes[0] = 0;
        nextFreeOffsetBytes[1] = 8;
        this.write_data(6, nextFreeOffsetBytes);

        return;
    }
    // returns the keys in the node - will be evaluated
    @Override
    public T[] getKeys() {

        int numKeys = getNumKeys();
        T[] keys = (T[]) new Object[numKeys];

        int off = 8;
        /* Write your code here */
        for (int i=0; i<numKeys; i++) {
            byte[] key_length_bytes = this.get_data(off +2, 2);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8)| (key_length_bytes[1] & 0xFF);
            byte[] key_bytes = this.get_data(off + 4, key_length);
            keys[i] = (T) convertBytesToT(key_bytes, typeClass);
            off += 2 + 2 + key_length;
        }

        return keys;

    }

    // returns the block ids in the node - will be evaluated
    public int[] getBlockIds() {

        int numKeys = getNumKeys();

        int[] block_ids = new int[numKeys];

        /* Write your code here */
        int off = 8;
        /* Write your code here */
        for (int i=0; i<numKeys; i++) {
            byte[] key_length_bytes = this.get_data(off +2, 2);
            int key_length = (key_length_bytes[0]  & 0xFF) << 8 | (key_length_bytes[1] & 0xFF);
            byte[] block_id_bytes = this.get_data(off, 2);
            block_ids[i] = (block_id_bytes[0]  & 0xFF) << 8 | (block_id_bytes[1] & 0xFF);
            off += 2 + 2 + key_length;
        }

        return block_ids;
    }
    public int getFreeOffset() {
        byte[] numKeysBytes = this.get_data(6, 2);
        return ((numKeysBytes[0]& 0xFF) << 8) | (numKeysBytes[1] & 0xFF);
    }
    public void setFreeOffset(int offset) {
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) ((offset >> 8) & 0xFF); // Most significant byte
        numKeysBytes[1] = (byte) (offset & 0xFF);       // Least significant byte
        this.write_data(6, numKeysBytes);       // Write the bytes to the node
    }

    public void setNext(int block_id) {
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) ((block_id >> 8) & 0xFF); // Most significant byte
        numKeysBytes[1] = (byte) (block_id & 0xFF);      // Least significant byte
        this.write_data(4, numKeysBytes);       // Write the bytes to the node
    }
    public int getNext() {
        byte[] numKeysBytes = this.get_data(4, 2);
        return ((numKeysBytes[0] & 0xFF) << 8) | (numKeysBytes[1] & 0xFF);
    }
    public int getPrev() {
        byte[] numKeysBytes = this.get_data(2, 2);
        return ((numKeysBytes[0] & 0xFF) << 8)   | (numKeysBytes[1] & 0xFF);
    }
    public void setPrev(int block_id) {
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) ((block_id >> 8) & 0xFF); // Most significant byte
        numKeysBytes[1] = (byte) (block_id & 0xFF);      // Least significant byte
        this.write_data(2, numKeysBytes);       // Write the bytes to the node
    }

    public void setNumKeys(int numKeys) {
        byte[] numKeysBytes = new byte[2];
        numKeysBytes[0] = (byte) ((numKeys >> 8) & 0xFF); // Most significant byte
        numKeysBytes[1] = (byte) (numKeys & 0xFF);       // Least significant byte
        this.write_data(0, numKeysBytes);       // Write the bytes to the node
    }

    // can be used as helper function - won't be evaluated

    @Override
    public void insert(T key, int block_id) {

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

        int off = 8;
        for (int i=0; i<insertIndex; i++) {
            byte[] key_length_bytes = this.get_data(off +2, 2);
            int key_length = ((key_length_bytes[0] & 0xFF) << 8) | (key_length_bytes[1] & 0xFF);
            off += 2 + 2 + key_length;
        }

        byte[] remaining_data;
        remaining_data = this.get_data(off,empty-off );

        byte[] keyBytes = convertTToBytes(key, typeClass);
        byte[] blockIdBytes = new byte[2];
        blockIdBytes[0] = (byte) ((block_id >> 8) & 0xFF);
        blockIdBytes[1] = (byte) (block_id & 0xFF);
        int new_key_length = keyBytes.length;
        byte[] new_key_length_bytes = new byte[2];
        new_key_length_bytes[0] = (byte) ((new_key_length >> 8) & 0xFF);
        new_key_length_bytes[1] = (byte) (new_key_length & 0xFF);

        this.write_data(off, blockIdBytes);
        this.write_data(off+2, new_key_length_bytes);
        this.write_data(off+4, keyBytes);

        this.write_data(off+4+new_key_length, remaining_data);
        setFreeOffset(empty+4+new_key_length);
        numKeys++;
        this.setNumKeys(numKeys);
        return;
    }

    // can be used as helper function - won't be evaluated
    @Override
    public int search(T key) {

        T[] keys = this.getKeys();

        for (int i = 0; i < (this.get_data(0,2) [0] << 8 | this.get_data(0,2)[1] & 0xFF); i++) {
            if (key.equals(keys[i])) {
                return this.getBlockIds()[i];
            }
        }
        return -1;
    }

}
