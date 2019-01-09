package cluster;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class TableEtc {
    private Configuration conf;
    private Connection connection;
    private Admin admin;

    public TableEtc() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "cloud024:60000");
        conf.set("fs.defaultFS", "hdfs://cloud024:9000");
        FileSystem fileSystem = FileSystem.newInstance(conf);
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }


    public static void main(String[] args) throws IOException {
        TableEtc tableEtc = new TableEtc();
//        tableEtc.tableCreate("txyl", new String[]{"xingming", "nianling"});
        tableEtc.tableScan("ID-TimestampDemo");
    }

    public List<String> list() throws IOException {
        TableName[] tableNames = admin.listTableNames();
        List<String> tables = new ArrayList<>();
        for(TableName tableName:tableNames){
            tables.add(tableName.toString());
        }
        return tables;
    }

    public Table getTable(String StringTableName) throws IOException {
        return connection.getTable(TableName.valueOf(StringTableName));
    }

    public void tableCreate(String tableNameString, String[] familyColumnNames) throws IOException {
        TableName tableName = TableName.valueOf(tableNameString);
        if(!admin.tableExists(tableName)) {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            for (String family : familyColumnNames) {
                columnFamilyDescriptors.add(ColumnFamilyDescriptorBuilder.of(family));
            }
            tableDescriptorBuilder.setColumnFamilies(columnFamilyDescriptors);
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("Table " + tableNameString + " has been created successfully!");
        }else {
            System.out.println("Fail to create " + tableNameString + ", it has existed!...");
        }
    }


    public void tableDisable(String tableNameString) throws IOException {
        TableName tableName = TableName.valueOf(tableNameString);
        if(!admin.isTableDisabled(tableName)) {
            admin.disableTable(tableName);
            System.out.println(tableNameString + " is now disabled...");
        }else {
            System.out.println(tableNameString + " has been disabled. There is no need to disable it.");
        }
    }

    public void enableTable(String tableNameString){
        TableName tableName = TableName.valueOf(tableNameString);
        try {
            if(admin.isTableDisabled(tableName)){
                admin.enableTable(tableName);
                System.out.println(tableNameString + " is now enabled...");
            }else {
                System.out.println(tableNameString + " has been enabled. There is no need to enable it.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void tableDrop(String tableNameString) throws IOException {
        TableName tableName = TableName.valueOf(tableNameString);
        if(admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println(tableNameString + " has been dropped!");
        }else {
            System.out.println("No " + tableNameString + " exist!");
        }
    }

    public void addColumnFamilies(String tableNameString, String[] familyColumnNames) throws IOException {
        TableName tableName = TableName.valueOf(tableNameString);
        admin.disableTable(tableName);
        for(String cf:familyColumnNames){
            admin.addColumnFamily(tableName, ColumnFamilyDescriptorBuilder.of(cf));
        }
        admin.enableTable(tableName);
    }

    public void deleteColumnFamilies(String tableNameString, String[] familyColumnNames) throws IOException {
        TableName tableName = TableName.valueOf(tableNameString);
        admin.disableTable(tableName);
        for(String cf:familyColumnNames){
            admin.deleteColumnFamily(tableName, Bytes.toBytes(cf));
        }
        admin.enableTable(tableName);
    }

    void tableScan(String tableNameString) throws IOException {
        System.out.println("Scanning table " + tableNameString + "...:");
        Table table = this.getTable(tableNameString);
        Scan scan = new Scan();

        ResultScanner resultScanners = table.getScanner(scan);
        for (Result resultScanner : resultScanners) {
            System.out.println("RowKey:" + new String(resultScanner.getRow()));
            Map<String, Map<String, String>> resultMap = new HashMap<>();
            resultScanner.listCells().forEach(
                    cell -> {
                        int familyOffset = cell.getFamilyOffset();
                        int familyLength = cell.getFamilyLength();
                        byte[] fullFamily = cell.getFamilyArray();
                        String familyString = new String(ArrayUtils.subarray(fullFamily, familyOffset, familyOffset + familyLength));

                        int qualifierOffset = cell.getQualifierOffset();
                        int qualifierLength = cell.getQualifierLength();
                        byte[] fullQualifier = cell.getQualifierArray();
                        String qualifierString = new String(ArrayUtils.subarray(fullQualifier, qualifierOffset, qualifierOffset + qualifierLength));

                        int valueOffset = cell.getValueOffset();
                        int valueLength = cell.getValueLength();
                        byte[] fullValue = cell.getValueArray();
                        String valueString = new String(ArrayUtils.subarray(fullValue, valueOffset, valueOffset + valueLength));

                        resultMap.putIfAbsent(familyString, new HashMap<>());
                        resultMap.get(familyString).putIfAbsent(qualifierString, valueString);
                    }
            );
            resultMap.forEach((family, qualifierAndValueMap) -> {
                int familyLength = family.length();
                boolean outputFamily = true;
                Set<String> keySet = qualifierAndValueMap.keySet();
                List<String> keyList = new ArrayList<>(keySet);
                keyList.sort(Comparator.naturalOrder());
                for(String qualifier: keyList){
                    if (outputFamily){
                        System.out.print(family);
                        outputFamily = false;
                    }else {
                        for(int i = 0; i < familyLength; i++){
                            System.out.print(" ");
                        }
                    }
                    System.out.println(" -> " + qualifier + " -> " + qualifierAndValueMap.get(qualifier));
                }
                System.out.println();
            });
        }
    }



}
