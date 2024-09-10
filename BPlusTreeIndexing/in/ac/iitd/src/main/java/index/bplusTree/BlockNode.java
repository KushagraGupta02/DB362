package index.bplusTree;

import storage.AbstractBlock;

// Extends AbstractBlock, will be extended by InternalNode and LeafNode
public class BlockNode extends AbstractBlock {
    public static int counter=0;
    public BlockNode(byte[] data) {
        super(data);
    }

    public BlockNode() {
        super();
    }

    public int getNumKeys() {
        counter+=1;
        byte[] numKeysBytes = this.get_data(0, 2);
        return (numKeysBytes[0] << 8) | (numKeysBytes[1] & 0xFF);
    }
}