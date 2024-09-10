package manager;

import index.bplusTree.BPlusTreeIndexFile;
import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;

    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }


    // converts a row to byte array to write to relational file
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    public int get_file_id (String table_name) {
        return file_to_fileid.get(table_name);
    }
    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {

                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);

                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {

                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);

                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        if (!check_file_exists(table_name)) {
            return null;
        }
        byte [] block_data = get_data_block(table_name, block_id);

        if (block_data == null) {
            return null;
        }
        int off = 0;

        byte [] meta_block_data = get_data_block(table_name, 0);
        int num_columns = (meta_block_data[1] & 0xFF) << 8 |  (meta_block_data[0] & 0xFF);
        // string - 0, int -1, boolean -2, float -3, double -4
        int v = 0;
        List<Object[]> answer = new ArrayList<>();
        List<Integer> types_list = new ArrayList<>();
        for (int i= 0; i< num_columns; i++) {
            int column_offset_start = 2 + i * 2;
            int column_id = (meta_block_data[column_offset_start+1] & 0xFF) << 8 | (meta_block_data[column_offset_start] & 0xFF);
            int data_type = meta_block_data[column_id];
            if (data_type == 0){
                v++;
            }
            else{
                types_list.add(data_type);
            }
            int column_name_length = meta_block_data[column_id + 1];
            String column_name = new String(meta_block_data, column_id + 2, column_name_length);
        }

        int num_records = (block_data[0] & 0xFF)<< 8 | (block_data[1] & 0xFF);
        for (int i = 0; i <num_records; i++) {
            int record_offset = i * 2 + 2;
            int record_id = (block_data[record_offset] & 0xFF)<< 8 | (block_data[record_offset + 1] & 0xFF);
            int fixed_offset = record_id + 4*v;
            Object[] entry = new Object[num_columns];
            for (int f=0; f<num_columns-v;f++){
                if (types_list.get(f) == 1){
                    int val = (block_data[fixed_offset+3] & 0xFF)<< 24 | (block_data[fixed_offset + 2]& 0xFF) << 16 | (block_data[fixed_offset + 1]& 0xFF) << 8 | (block_data[fixed_offset] & 0xFF);
                    entry[f] = val;
                    fixed_offset += 4;
                }
                else if (types_list.get(f) == 2){
                    boolean val = block_data[fixed_offset] != 0;
                    entry[f] = val;
                    fixed_offset += 1;
                }
                else if (types_list.get(f) == 3){
                    float val = Float.intBitsToFloat((block_data[fixed_offset+3] & 0xFF) << 24 | (block_data[fixed_offset + 2]& 0xFF) << 16 | (block_data[fixed_offset + 1] & 0xFF)<< 8 | (block_data[fixed_offset] & 0xFF));
                    entry[f] = val;
                    fixed_offset += 4;
                }
                else if (types_list.get(f) == 4){
                    double val = Double.longBitsToDouble((long)(block_data[fixed_offset+7] & 0xFF) << 56 | (long)(block_data[fixed_offset + 6]& 0xFF) << 48 | (long)(block_data[fixed_offset + 5]& 0xFF)<< 40 |(long) (block_data[fixed_offset + 4]& 0xFF) << 32 | (long)(block_data[fixed_offset + 3]& 0xFF) << 24 | (long) (block_data[fixed_offset + 2] & 0xFF) << 16 |(long) (block_data[fixed_offset + 1] & 0xFF)<< 8 | (long) (block_data[fixed_offset] & 0xFF));
                    entry[f] = val;
                    fixed_offset += 8;
                }
                else{
                    return null;
                }
            }
            int variable_offset = record_id;
            for (int v_temp = 0; v_temp<v; v_temp++){
                int offset = (block_data[variable_offset + 1] & 0xFF )<< 8 | (block_data[variable_offset] & 0xFF);
                int length = (block_data[variable_offset + 3] & 0xFF) << 8 | (block_data[variable_offset+2] & 0xFF);
                String val = new String(block_data, record_id +offset, length);
                entry[num_columns-v+v_temp] = val;
                variable_offset += 4;
            }
            // now null bitmap
            int bitmap_offset = fixed_offset;
            for (int f=0; f<num_columns; f++){
                if ((block_data[bitmap_offset] & (1 << (7 - f%8))) != 0){
                    entry[f] = null;
                }
                if (f%8 == 7){
                    bitmap_offset++;
                }
            }
            answer.add(entry);
        }
        return answer;
    }

    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */
        // Hint: You need to create an index file and write the index blocks
        int file_id = get_file_id(table_name);
        byte [] meta_block_data = get_data_block(table_name, 0);
        int num_columns = (meta_block_data[1] & 0xFF )<< 8 |  (meta_block_data[0] & 0xFF);
        // string - 0, int -1, boolean -2, float -3, double -4
        int column_index = -1;
        int get_data = -1;
        for (int i= 0; i< num_columns; i++) {
            int column_offset_start = 2 + i * 2;
            int column_id = (meta_block_data[column_offset_start+1] & 0xFF) << 8 | (meta_block_data[column_offset_start] & 0xFF);
            int data_type = meta_block_data[column_id];
            int column_name_length = meta_block_data[column_id + 1];
            String new_column_name = new String(meta_block_data, column_id + 2, column_name_length);
            if (new_column_name.equals(column_name)){
                get_data = data_type;
                column_index = i;
                break;
            }
        }
        BPlusTreeIndexFile index;
        if (get_data == 0){
            index = new BPlusTreeIndexFile(order, String.class);
        }
        else if (get_data == 1){
            // then class<Integer>
            index = new BPlusTreeIndexFile(order, Integer.class);
        }
        else if (get_data == 2){
            // then class<Boolean>
            index = new BPlusTreeIndexFile(order, Boolean.class);
        }
        else if (get_data == 3){
            // then class<Float>
            index = new BPlusTreeIndexFile(order, Float.class);
        }
        else if (get_data == 4){
            index = new BPlusTreeIndexFile(order, Double.class);
        }
        else{
            return false;
        }
        int i = 1;
        while (db.get_data(file_id, i) != null){
            List<Object[]> records = get_records_from_block(table_name, i);
            for (Object[] record: records){
                if (record[column_index] == null){
                    continue;
                }
                if (get_data == 0){
                    index.insert((String.class).cast(record[column_index]), i);
                }
                else if (get_data == 1){
                    // then class<Integer>
                    index.insert((Integer.class).cast(record[column_index]), i);
                }
                else if (get_data == 2){
                    // then class<Boolean>
                    index.insert((Boolean.class).cast(record[column_index]), i);
                }
                else if (get_data == 3){
                    // then class<Float>
                    index.insert((Float.class).cast(record[column_index]), i);
                }
                else if (get_data == 4){
                    index.insert((Double.class).cast(record[column_index]), i);
                }
                else{
                    return false;
                }
            }
            i++;
        }
//        index. BFS_new();
        int counter = db.addFile(index);
        file_to_fileid.put(table_name + "_" + column_name + "_index", counter);
        return true;
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to search in the index file
        if (!check_index_exists(table_name, column_name)){
            return -1;
        }
        int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
        if (value.getType().getSqlTypeName().getName().equals("INTEGER")){
            return db.search_index(file_id, value.getValueAs(Integer.class));
        }
        else if (value.getType().getSqlTypeName().getName().equals("VARCHAR")){
            return db.search_index(file_id, value.getValueAs(String.class));
        }
        else if (value.getType().getSqlTypeName().getName().equals("CHAR")){
            return db.search_index(file_id, value.getValueAs(String.class));
        }
        else if (value.getType().getSqlTypeName().getName().equals("STRING")){
            return db.search_index(file_id, value.getValueAs(String.class));
        }
        else if (value.getType().getSqlTypeName().getName().equals("BOOLEAN")){
            return db.search_index(file_id, value.getValueAs(Boolean.class));
        }
        else if (value.getType().getSqlTypeName().getName().equals("FLOAT")){
            return db.search_index(file_id, value.getValueAs(Float.class));
        }
        else if (value.getType().getSqlTypeName().getName().equals("DOUBLE")){
            return db.search_index(file_id, value.getValueAs(Double.class));
        }
        else if (value.getType().getSqlTypeName().getName().equals("DECIMAL")){
            return db.search_index(file_id, value.getValueAs(Double.class));
        }
        return -1;
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
//            System.out.println("Index does not exist");
        }
        return null;
    }

}