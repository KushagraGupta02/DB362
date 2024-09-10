package optimizer.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import manager.StorageManager;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {

    private final List<RexNode> projects;
    private final RelDataType rowType;
    private final RelOptTable table;
    private final RexNode filter;

    public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
        super(cluster, traitSet, table);
        this.table = table;
        this.rowType = deriveRowType();
        this.filter = filter;
        this.projects = projects;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PIndexScan(getCluster(), traitSet, table, filter, projects);
    }

    @Override
    public RelOptTable getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "PIndexScan";
    }

    public String getTableName() {
        return table.getQualifiedName().get(1);
    }

    public Object cast_fetch_key(byte [] block, int off, int length, SqlTypeName type){
        if(type == SqlTypeName.INTEGER){
            return (Integer) ByteBuffer.wrap(block, off, length).getInt();
        }
        else if(type == SqlTypeName.VARCHAR){
            return new String(block, off, length);
        }
        else if(type == SqlTypeName.CHAR){
            return new String(block, off, length);
        }
        else if(type == SqlTypeName.DOUBLE){
            return (Double) ByteBuffer.wrap(block, off, length).getDouble();
        }
        else if(type == SqlTypeName.DECIMAL){
            return (Double) ByteBuffer.wrap(block, off, length).getDouble();
        }
        else if(type == SqlTypeName.BOOLEAN){
            return (Boolean) (block[off] != 0);
        }
        else if(type == SqlTypeName.FLOAT){
            return (Float) ByteBuffer.wrap(block, off, length).getFloat();
        }
        return null;
    }

    public int comparator (Object first, Object second, SqlTypeName type){
        if (type == SqlTypeName.INTEGER)  {
            return Integer.compare((Integer) first, (Integer) second);
        }
        else if (type == SqlTypeName.VARCHAR){
            return ((String) first).compareTo((String) second);
        }
        else if(type == SqlTypeName.CHAR){
            return ((String) first).compareTo((String) second);
        }
        else if(type == SqlTypeName.DOUBLE){
            return Double.compare((Double) first, (Double) second);
        }
        else if(type == SqlTypeName.DECIMAL){
            return Double.compare((Double) first, (Double) second);
        }
        else if(type == SqlTypeName.BOOLEAN){
            boolean a = (Boolean) first;
            boolean b = (Boolean) second;
            if (a == b) {
                return 0;
            } else if (a) {
                return 1;
            } else {
                return -1;
            }
        }
        else if(type == SqlTypeName.FLOAT){
            return Float.compare((Float) first, (Float) second);
        }
        return 0;
    }

    // where clause parse and get block from records, fetch columns
    @Override
    public List<Object[]> evaluate(StorageManager storage_manager) {
        String tableName = this.getTableName();
        System.out.println("Evaluating PIndexScan for table: " + tableName);
        RexNode filter = this.filter;

        if (filter instanceof RexCall) {
            RexCall call = (RexCall) filter;

            // Check the type of operation
            SqlKind kind = call.getKind();
            RexNode operand0 = call.getOperands().get(0);
            RexNode operand1 = call.getOperands().get(1);
            if (operand0 instanceof RexInputRef && operand1 instanceof RexLiteral) {
                int column_index = -1;
                int get_data = -1;
                String column_name = null;
                List<Object[]> answer = new ArrayList<>();
                byte[] meta_block_data = storage_manager.get_data_block(tableName, 0);
                int num_columns = (meta_block_data[1] & 0xFF) << 8 | (meta_block_data[0] & 0xFF);
                RexInputRef inputRef = (RexInputRef) operand0;
                int columnIndex = inputRef.getIndex();
                String columnName = rowType.getFieldNames().get(columnIndex);
                RexLiteral literal = (RexLiteral) operand1;
                Object value = literal.getValue();
                SqlTypeName typeName = rowType.getFieldList().get(columnIndex).getType().getSqlTypeName();
                switch (typeName){
                    case INTEGER:
                        value = literal.getValueAs(Integer.class);
                        break;
                    case BOOLEAN:
                        value = literal.getValueAs(Boolean.class);
                        break;
                    case CHAR:
                    case VARCHAR:
                        value = literal.getValueAs(String.class);
                        break;
                    case DOUBLE:
                    case DECIMAL:
                        value = literal.getValueAs(Double.class);
                        break;
                    case FLOAT:
                        value = literal.getValueAs(Float.class);
                        break;
                    default:
//                            System.out.println("Invalid type");
                        break;
                }
                column_name = columnName;
                for (int i = 0; i < num_columns; i++) {
                    int column_offset_start = 2 + i * 2;
                    int column_id = (meta_block_data[column_offset_start + 1] & 0xFF) << 8 | (meta_block_data[column_offset_start] & 0xFF);
                    int data_type = meta_block_data[column_id];
                    int column_name_length = meta_block_data[column_id + 1];
                    String new_column_name = new String(meta_block_data, column_id + 2, column_name_length);
                    if (new_column_name.equals(columnName)) {
                        get_data = data_type;
                        column_index = i;
                        break;
                    }
                }
                int file_index_id = storage_manager.get_file_id(tableName + "_" + columnName + "_index");
                int file_id = storage_manager.get_file_id(tableName);
                if (!storage_manager.check_index_exists(tableName, column_name)) {
                    return answer;
                }
                if (filter != null){
                    if (kind == SqlKind.EQUALS) {

                        int leafNode = storage_manager.search(tableName, column_name, literal);
                        if (leafNode == -1) {
                            return answer;
                        }

                        HashSet<Integer> block_ids = new HashSet<Integer>();
                        List <Integer> fetch_records = new ArrayList<>();
                        int currNode = leafNode;
                        int flag = 0;
                        while (currNode!=0 && flag==0) {
                            byte []  leaf_node_bytes = storage_manager.get_data_block(tableName + "_" + column_name + "_index", currNode);
                            int numKeys = (leaf_node_bytes[0] & 0xFF) << 8 | (leaf_node_bytes[1] & 0xFF);
                            int prev = (leaf_node_bytes[2] & 0xFF) << 8 | (leaf_node_bytes[3] & 0xFF);
                            int next = (leaf_node_bytes[4] & 0xFF) << 8 | (leaf_node_bytes[5] & 0xFF);
                            int[] leaf_block_ids = new int[numKeys];
                            Object [] keys = new Object[numKeys];
                            int off = 8;
                            /* Write your code here */
                            for (int i = 0; i < numKeys; i++) {
                                int key_length = ((leaf_node_bytes[off + 2] & 0xFF) << 8) | (leaf_node_bytes[off + 3] & 0xFF);
                                leaf_block_ids[i] = ((leaf_node_bytes[off] & 0xFF) << 8) | (leaf_node_bytes[off + 1] & 0xFF);
                                keys[i] = cast_fetch_key(leaf_node_bytes, off + 4, key_length, typeName);
                                off += 2 + 2 + key_length;

                            }
                            for (int i = 0; i < numKeys; i++) {
                                if (!block_ids.contains(leaf_block_ids[i]) && comparator(keys[i],value,typeName)==0){
                                    fetch_records.add(leaf_block_ids[i]);
                                    block_ids.add(leaf_block_ids[i]);
                                }
                                else if (comparator(keys[i],value,typeName)>0){
                                    flag++;
                                    break;
                                }
                            }
                            currNode = next;
                        }
                        for (int i = 0; i < fetch_records.size(); i++) {
                            List<Object[]> records = storage_manager.get_records_from_block(tableName, fetch_records.get(i));
                            for (int j = 0; j < records.size(); j++) {
                                Object[] record = records.get(j);
                                if (record[column_index]!=null && comparator(record[column_index],value,typeName)==0) {
                                    answer.add(record);
                                }
                            }
                        }
                    }
                    else if (kind == SqlKind.GREATER_THAN) {
                        int leafNode = storage_manager.search(tableName, column_name, literal);
                        if (leafNode == -1) {
                            return answer;
                        }
                        HashSet<Integer> block_ids = new HashSet<Integer>();
                        List <Integer> fetch_records = new ArrayList<>();
                        int currNode = leafNode;
                        while (currNode!=0) {
                            byte[] leaf_node_bytes = storage_manager.get_data_block(tableName + "_" + columnName + "_index", currNode);
                            int numKeys = (leaf_node_bytes[0] & 0xFF) << 8 | (leaf_node_bytes[1] & 0xFF);
                            int prev = (leaf_node_bytes[2] & 0xFF) << 8 | (leaf_node_bytes[3] & 0xFF);
                            int next = (leaf_node_bytes[4] & 0xFF) << 8 | (leaf_node_bytes[5] & 0xFF);
                            int[] leaf_block_ids = new int[numKeys];
                            Object [] keys = new Object[numKeys];
                            int off = 8;
                            /* Write your code here */
                            for (int i = 0; i < numKeys; i++) {
                                int key_length = ((leaf_node_bytes[off + 2] & 0xFF) << 8) | (leaf_node_bytes[off + 3] & 0xFF);
                                leaf_block_ids[i] = ((leaf_node_bytes[off] & 0xFF) << 8) | (leaf_node_bytes[off + 1] & 0xFF);
                                keys[i] = cast_fetch_key(leaf_node_bytes, off + 4, key_length, typeName);
                                off += 2 + 2 + key_length;

                            }
                            for (int i = 0; i < numKeys; i++) {
                                if (!block_ids.contains(leaf_block_ids[i]) && comparator(keys[i],value,typeName)>0){
                                    fetch_records.add(leaf_block_ids[i]);
                                    block_ids.add(leaf_block_ids[i]);
                                }
                            }
                            currNode = next;
                        }
                        for (int i = 0; i < fetch_records.size(); i++) {
                            List<Object[]> records = storage_manager.get_records_from_block(tableName, fetch_records.get(i));
                            for (int j = 0; j < records.size(); j++) {
                                Object[] record = records.get(j);
                                if (record[column_index]!=null && comparator(record[column_index],value,typeName)>0) {
                                    answer.add(record);
                                }
                            }
                        }

                    }
                    else if (kind == SqlKind.GREATER_THAN_OR_EQUAL) {
                        int leafNode = storage_manager.search(tableName, column_name, literal);
                        if (leafNode == -1) {
                            return answer;
                        }
                        HashSet<Integer> block_ids = new HashSet<Integer>();
                        List <Integer> fetch_records = new ArrayList<>();
                        int currNode = leafNode;
                        while (currNode!=0) {
                            byte[]   leaf_node_bytes = storage_manager.get_data_block(tableName + "_" + columnName + "_index", currNode);
                            int numKeys = (leaf_node_bytes[0] & 0xFF) << 8 | (leaf_node_bytes[1] & 0xFF);
                            int prev = (leaf_node_bytes[2] & 0xFF) << 8 | (leaf_node_bytes[3] & 0xFF);
                            int next = (leaf_node_bytes[4] & 0xFF) << 8 | (leaf_node_bytes[5] & 0xFF);
                            int[] leaf_block_ids = new int[numKeys];
                            Object [] keys = new Object[numKeys];
                            int off = 8;
                            /* Write your code here */
                            for (int i = 0; i < numKeys; i++) {
                                int key_length = ((leaf_node_bytes[off + 2] & 0xFF) << 8) | (leaf_node_bytes[off + 3] & 0xFF);
                                leaf_block_ids[i] = ((leaf_node_bytes[off] & 0xFF) << 8) | (leaf_node_bytes[off + 1] & 0xFF);
                                keys[i] = cast_fetch_key(leaf_node_bytes, off + 4, key_length, typeName);
                                off += 2 + 2 + key_length;

                            }
                            for (int i = 0; i < numKeys; i++) {
                                if (!block_ids.contains(leaf_block_ids[i]) && comparator(keys[i],value,typeName)>=0){
                                    fetch_records.add(leaf_block_ids[i]);
                                    block_ids.add(leaf_block_ids[i]);
                                }
                            }
                            currNode = next;
                        }
                        for (int i = 0; i < fetch_records.size(); i++) {
                            List<Object[]> records = storage_manager.get_records_from_block(tableName, fetch_records.get(i));
                            for (int j = 0; j < records.size(); j++) {
                                Object[] record = records.get(j);
                                if (record[column_index]!=null && comparator(record[column_index],value,typeName)>=0) {
                                    answer.add(record);
                                }
                            }
                        }
                    }
                    else if (kind == SqlKind.LESS_THAN) {
                        int leafNode = storage_manager.search(tableName, column_name, literal);
                        if (leafNode == -1) {
                            int num_blocks = 1;
                            while (storage_manager.getDb().get_data(file_id, num_blocks) != null) {
                                List<Object[]> records = storage_manager.get_records_from_block(tableName, num_blocks);
                                for(Object[] rec : records){ answer.add(rec);}
                                num_blocks++;
                            }
                            return answer;
                        }
                        HashSet<Integer> block_ids = new HashSet<Integer>();
                        List <Integer> fetch_records = new ArrayList<>();
                        int currNode = leafNode;
                        while (currNode!=0) {
                            byte[]   leaf_node_bytes = storage_manager.get_data_block(tableName + "_" + columnName + "_index", currNode);
                            int numKeys = (leaf_node_bytes[0] & 0xFF) << 8 | (leaf_node_bytes[1] & 0xFF);
                            int prev = (leaf_node_bytes[2] & 0xFF) << 8 | (leaf_node_bytes[3] & 0xFF);
                            int next = (leaf_node_bytes[4] & 0xFF) << 8 | (leaf_node_bytes[5] & 0xFF);
                            int[] leaf_block_ids = new int[numKeys];
                            Object [] keys = new Object[numKeys];
                            int off = 8;
                            /* Write your code here */
                            for (int i = 0; i < numKeys; i++) {
                                int key_length = ((leaf_node_bytes[off + 2] & 0xFF) << 8) | (leaf_node_bytes[off + 3] & 0xFF);
                                leaf_block_ids[i] = ((leaf_node_bytes[off] & 0xFF) << 8) | (leaf_node_bytes[off + 1] & 0xFF);
                                keys[i] = cast_fetch_key(leaf_node_bytes, off + 4, key_length, typeName);
                                off += 2 + 2 + key_length;

                            }
                            for (int i = numKeys-1; i >= 0; i--) {
                                if (!block_ids.contains(leaf_block_ids[i]) && comparator(keys[i],value,typeName)<0){
                                    fetch_records.add(leaf_block_ids[i]);
                                    block_ids.add(leaf_block_ids[i]);
                                }
                            }
                            currNode = prev;
                        }
                        for (int i = 0; i < fetch_records.size(); i++) {
                            List<Object[]> records = storage_manager.get_records_from_block(tableName, fetch_records.get(i));
                            for (int j = 0; j < records.size(); j++) {
                                Object[] record = records.get(j);
                                if (record[column_index]!=null && comparator(record[column_index],value,typeName)<0) {
                                    answer.add(record);
                                }
                            }
                        }
                    }
                    else if (kind == SqlKind.LESS_THAN_OR_EQUAL) {

                        int leafNode = storage_manager.search(tableName, column_name, literal);
                        if (leafNode == -1) {
                            int num_blocks = 1;
                            while (storage_manager.getDb().get_data(file_id, num_blocks) != null) {
                                List<Object[]> records = storage_manager.get_records_from_block(tableName, num_blocks);
                                for(Object[] rec : records){ answer.add(rec);}
                                num_blocks++;
                            }
                            return answer;
                        }
                        HashSet<Integer> block_ids = new HashSet<Integer>();
                        List <Integer> fetch_records = new ArrayList<>();
                        int currNode = leafNode;
                        while (currNode!=0) {
                            byte[]   leaf_node_bytes = storage_manager.get_data_block(tableName + "_" + columnName + "_index", currNode);
                            int numKeys = (leaf_node_bytes[0] & 0xFF) << 8 | (leaf_node_bytes[1] & 0xFF);
                            int prev = (leaf_node_bytes[2] & 0xFF) << 8 | (leaf_node_bytes[3] & 0xFF);
                            int[] leaf_block_ids = new int[numKeys];
                            Object [] keys = new Object[numKeys];
                            int off = 8;
                            /* Write your code here */
                            for (int i = 0; i < numKeys; i++) {
                                int key_length = ((leaf_node_bytes[off + 2] & 0xFF) << 8) | (leaf_node_bytes[off + 3] & 0xFF);
                                leaf_block_ids[i] = ((leaf_node_bytes[off] & 0xFF) << 8) | (leaf_node_bytes[off + 1] & 0xFF);
                                keys[i] = cast_fetch_key(leaf_node_bytes, off + 4, key_length, typeName);
                                off += 2 + 2 + key_length;

                            }
                            for (int i = numKeys-1; i >= 0; i--) {
                                if (!block_ids.contains(leaf_block_ids[i]) && comparator(keys[i],value,typeName)<=0){
                                    fetch_records.add(leaf_block_ids[i]);
                                    block_ids.add(leaf_block_ids[i]);
                                }
                            }
                            currNode = prev;
                        }

                        int currNodenew = leafNode;
                        while (currNodenew!=0) {
                            byte[] leaf_node_bytes = storage_manager.get_data_block(tableName + "_" + columnName + "_index", currNodenew);
                            int numKeys = (leaf_node_bytes[0] & 0xFF) << 8 | (leaf_node_bytes[1] & 0xFF);
                            int prev = (leaf_node_bytes[2] & 0xFF) << 8 | (leaf_node_bytes[3] & 0xFF);
                            int next = (leaf_node_bytes[4] & 0xFF) << 8 | (leaf_node_bytes[5] & 0xFF);
                            int[] leaf_block_ids = new int[numKeys];
                            Object [] keys = new Object[numKeys];
                            int off = 8;
                            /* Write your code here */
                            for (int i = 0; i < numKeys; i++) {
                                int key_length = ((leaf_node_bytes[off + 2] & 0xFF) << 8) | (leaf_node_bytes[off + 3] & 0xFF);
                                leaf_block_ids[i] = ((leaf_node_bytes[off] & 0xFF) << 8) | (leaf_node_bytes[off + 1] & 0xFF);
                                keys[i] = cast_fetch_key(leaf_node_bytes, off + 4, key_length, typeName);
                                off += 2 + 2 + key_length;

                            }
                            for (int i = numKeys-1; i >= 0; i--) {
                                if (!block_ids.contains(leaf_block_ids[i]) && comparator(keys[i],value,typeName)==0){
                                    fetch_records.add(leaf_block_ids[i]);
                                    block_ids.add(leaf_block_ids[i]);
                                }
                            }
                            currNodenew = next;
                        }

                        for (int i = 0; i < fetch_records.size(); i++) {
                            List<Object[]> records = storage_manager.get_records_from_block(tableName, fetch_records.get(i));
                            for (int j = 0; j < records.size(); j++) {
                                Object[] record = records.get(j);
                                if (record[column_index]!=null && comparator(record[column_index],value,typeName)<=0) {
                                    answer.add(record);
                                }
                            }
                        }
                    }
                }
                return answer;
            }
        }
        return null;
    }
}