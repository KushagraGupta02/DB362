package index.bplusTree;
import java.nio.ByteBuffer;
import java.util.Arrays;

// TreeNode interface - will be implemented by InternalNode and LeafNode
public interface TreeNode <T> {

    public T[] getKeys();
    public void insert(T key, int block_id);

    public int search(T key);

    // DO NOT modify this - may be used for evaluation
    default public void print() {
        T[] keys = getKeys();
        for (T key : keys) {
            System.out.print(key + " ");
        }
        return;
    }
    default public int compareTo(Class<T> typeClass, T first, T second) {
        // Generic type T, int , float, double bool, B+ tree, byte array to type T
        /* Write your code here */
        if (typeClass == Integer.class) {
            return Integer.compare((Integer) first, (Integer) second);
        } else if (typeClass == Long.class) {
            return Long.compare((Long) first, (Long) second);
        } else if (typeClass == Double.class) {
            return Double.compare((Double) first, (Double) second);
        } else if (typeClass == Boolean.class) {
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
    default public byte[] convertTToBytes(T value, Class<T> typeClass) {
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
    default public T convertBytesToT(byte[] bytes, Class<T> typeClass) {
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

}