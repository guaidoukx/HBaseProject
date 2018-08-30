package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class WGS {
    private static Connection connection;
    private Table phoneEnrollInfo ;
    private Table DBD_ID ;
    private Table AssistTable;
    private Table ID_Timestamp;

    public WGS() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "10.141.209.224:60000");
        conf.set("fs.defaultFS", "hdfs://10.141.209.224:9000");
        connection = ConnectionFactory.createConnection(conf);
        phoneEnrollInfo = connection.getTable(TableName.valueOf("phoneEnrollInfo"));
        DBD_ID = connection.getTable(TableName.valueOf("DBD_ID-TS"));
        AssistTable = connection.getTable(TableName.valueOf("AssistTable"));
        ID_Timestamp = connection.getTable(TableName.valueOf("ID-Timestamp"));
    }


    List<Map<String,Map<String, Map<String,String>>>> phoneQuery(String phone)throws IOException {
        List<Map<String,Map<String, Map<String,String>>>> resultList = new ArrayList<>();
        int i = 1;
        Scan scanDBD_ID = new Scan();
        scanDBD_ID.withStartRow(Bytes.toBytes(phone+"_t"), true);
        scanDBD_ID.withStopRow(Bytes.toBytes(phone+"_t9"), true);
        ResultScanner resultScannerFilterList = DBD_ID.getScanner(scanDBD_ID);

        Get getID = new Get(Bytes.toBytes(phone));
        Result resultID = AssistTable.get(getID);
        if (resultID.size() != 0){
            byte[] ID = resultID.value();

            Get getAll = new Get(ID);
            Result resultAll = phoneEnrollInfo.get(getAll);
            if (resultAll.size() != 0) {
                System.out.println("------- " + i++ + " --------");
                /* to return a list */
                //resultList.add(this.resultFormat(resultAll));
                /* to print result */
//                this.printResult(this.resultFormat(resultAll));
            }else System.out.println("No ID matches the phone number " + phone);
        }else System.out.println("There are no records about the phone number "+phone);

        Result result;
        while ((result = resultScannerFilterList.next()) != null){
            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            //resultList.add(this.resultFormat(result));
            /* to print result */
//            this.printResult(this.resultFormat(result));
        }
        System.out.println("queryInRowkeyRange: There is no more row in this range! ");
        resultScannerFilterList.close();

        return resultList;
    }



    List<Map<String,Map<String, Map<String,String>>>> IDQueryToPhone(String ID)throws IOException {
        List<Map<String,Map<String, Map<String,String>>>> resultList = new ArrayList<>();

        int i = 1;
//        Scan scanID_TS = new Scan();
//        scanID_TS.withStartRow(Bytes.toBytes(ID+"-"), true);
//        scanID_TS.withStopRow(Bytes.toBytes(ID+"-9"), true);
//        ResultScanner resultScannerFilterList = ID_Timestamp.getScanner(scanID_TS);

        Get getAll = new Get(Bytes.toBytes(ID));
        Result resultAll = phoneEnrollInfo.get(getAll);
        if (resultAll.size() != 0) {
            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            //resultList.add(this.resultFormat(resultAll));
            /* to print result */
            this.printResult(this.resultFormat(resultAll));

            //some changes
            List<Cell> CellList = resultAll.getColumnCells(Bytes.toBytes("Info"), Bytes.toBytes("phoneNum"));
            String phone = Bytes.toString(CellUtil.cloneValue(CellList.get(0)));
            System.out.println(phone);

            Scan scanDBD_ID = new Scan();
            scanDBD_ID.withStartRow(Bytes.toBytes(phone+"_t"), true);
            scanDBD_ID.withStopRow(Bytes.toBytes(phone+"_t9"), true);
            ResultScanner resultScannerFilterList = DBD_ID.getScanner(scanDBD_ID);

            Result result;
            while ((result = resultScannerFilterList.next()) != null){
                System.out.println("------- " + i++ + " --------");
                /* to return a list */
                //resultList.add(this.resultFormat(result));
                /* to print result */
                this.printResult(this.resultFormat(result));
            }
            System.out.println("queryInRowkeyRange: There is no more row in this range! ");
            resultScannerFilterList.close();

        }else System.out.println("No items matches the ID " + ID);

        return resultList;
    }


    List<Map<String,Map<String, Map<String,String>>>> IDQuery(String ID)throws IOException {
        List<Map<String,Map<String, Map<String,String>>>> resultList = new ArrayList<>();

        int i = 1;
        Scan scanID_TS = new Scan();
        scanID_TS.withStartRow(Bytes.toBytes(ID+"-"), true);
        scanID_TS.withStopRow(Bytes.toBytes(ID+"-9"), true);
        ResultScanner resultScannerFilterList = ID_Timestamp.getScanner(scanID_TS);

        Get getAll = new Get(Bytes.toBytes(ID));
        Result resultAll = phoneEnrollInfo.get(getAll);
        if (resultAll.size() != 0) {
            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            //resultList.add(this.resultFormat(resultAll));
            /* to print result */
            this.printResult(this.resultFormat(resultAll));
        }else System.out.println("No items matches the ID " + ID);

        Result result;
        while ((result = resultScannerFilterList.next()) != null){
            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            //resultList.add(this.resultFormat(result));
            /* to print result */
            this.printResult(this.resultFormat(result));
        }
        System.out.println("queryInRowkeyRange: There is no more row in this range! ");
        resultScannerFilterList.close();

        return resultList;
    }

    /**
     * always used in a query method to get a format result.
     * @param result, No need to check if it is empty or not. This step should be in query method.
     * In a query method:
     *       1. when there's a result, call for @resultFormat method, return format result -> triple Map.
     *       2. otherwise, query method return @null.
     */
    public Map<String,Map<String, Map<String,String>>> resultFormat(Result result){
        List<Cell> cells = result.listCells();
        Map<String, Map<String, Map<String, String>>> resultMap = new HashMap<>();
        cells.forEach(
                cell -> {
                    String qualifierString = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String valueString = Bytes.toString(CellUtil.cloneValue(cell));
                    String familyString = Bytes.toString(CellUtil.cloneFamily(cell));
                    String rowkeyString = Bytes.toString(CellUtil.cloneRow(cell));
                    resultMap.putIfAbsent(rowkeyString, new HashMap<>());
                    resultMap.get(rowkeyString).putIfAbsent(familyString, new HashMap<>());
                    resultMap.get(rowkeyString).get(familyString).putIfAbsent(qualifierString, valueString);
                });
        return resultMap;
    }


    /**
     * used after a query method to print the format result out.
     * @param resultMap is either 'Map<String,Map<String, Map<String,String>>>' or @null, so check it first
     */
    public void printResult(Map<String,Map<String, Map<String,String>>>resultMap){
        if(resultMap == null) {
            System.out.println("printResult: ResultMap is empty! ");
        }else{
            Set<String> rowkeySet = resultMap.keySet();
            List<String> rowkeyList = new ArrayList<>(rowkeySet);
            rowkeyList.sort(Comparator.naturalOrder());
            for (String rowkey : rowkeyList) {
                System.out.println("rowkey -> " + rowkey);
                Set<String> familySet = resultMap.get(rowkey).keySet();
                List<String> familyList = new ArrayList<>(familySet);
                familyList.sort(Comparator.naturalOrder());
                for (String family : familyList) {
                    System.out.print("\t" + "columnFamily" + " -> " + family + "\n");
                    Set<String> qualifierSet = resultMap.get(rowkey).get(family).keySet();
                    List<String> qualifierList = new ArrayList<>(qualifierSet);
                    qualifierList.sort(Comparator.naturalOrder());
                    for (String qualifier : qualifierList) {
                        System.out.print("\t\t" + qualifier + " -> " + resultMap.get(rowkey).get(family).get(qualifier) + "\n");
                    }
                }
            }
            System.out.println();
        }
    }


    public static void main(String[] args) throws IOException {
        WGS wgs = new WGS();
//        Date b = new Date();
//        wgs.phoneQuery("13923170385");
        Date m = new Date();
//        System.out.println("---" + (m.getTime()-b.getTime()) + "ms ---\n");
        wgs.IDQuery("340203197007246194");
        Date a = new Date();
        System.out.println("---" + (a.getTime()-m.getTime()) + "ms ---\n");
    }
}

//import java.util.ArrayList;
//import java.util.List;
//
//public class WGS{
//    int enen(int i){
//        return i++;
//    }
//    public static void main(String[] args){
//        WGS wgs = new WGS();
//        List<Integer> list = new ArrayList<>();
//        int i = 0;
//        for (int x=0;x<5;x++){
//            i = wgs.enen(x);
//            list.add(i);
//        }
//        for (int m:list){
//            System.out.println(m);
//        }
//    }
//        }
