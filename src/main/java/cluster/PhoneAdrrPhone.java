package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PhoneAdrrPhone {
    static Connection connection;
    private FileSystem fileSystem;
    private QueryEtc queryEtc;
    private Table DBDPhone_T_TS;
    private Table AssistTable;
    private Table Addr_TS_ID;
    public PhoneAdrrPhone() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "cloud024:60000");
        conf.set("fs.defaultFS", "hdfs://cloud024:9000");
        fileSystem = FileSystem.newInstance(conf);
        connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        queryEtc = new QueryEtc();
        DBDPhone_T_TS = connection.getTable(TableName.valueOf("DBD_ID-TS"));
        AssistTable = connection.getTable(TableName.valueOf("AssistTable"));
        Addr_TS_ID = connection.getTable(TableName.valueOf("Addr_TS_ID"));
    }

    //1477332905
    //1442339515 13000015154_t5_1442339515 cl21 -> 上海市上海市杨浦区 cl39 -> 西藏那曲地区申扎县
    //1506627160 13000015154_t5_1506627160 cl21 -> 山东省临沂市沂水县 cl39 -> 台湾省桃园市其他
    public static void main(String[] args) throws IOException {
        PhoneAdrrPhone phoneAdrrPhone = new PhoneAdrrPhone();
        List<Map> a = phoneAdrrPhone.QueryDBDPhone_T_TS("13000015154", "1442339515", "1506627160");
        System.out.println(a);
//        phoneAdrrPhone.QueryAddr_TS_ID("上海市上海市其他", "1420725593", "1420725594");
    }



    List<Map> QueryDBDPhone_T_TS(String phone, String startTime, String stopTime) throws IOException {
        System.out.println();
        System.out.println("Phone: " + phone);
        System.out.println("Start Time: " + startTime);
        System.out.println("Stop Time: " + stopTime + "\n");

        int i = 1;
        String addrT = "_t5";
        String startQuery = phone + addrT + "_" + startTime;
        String stopQuery = phone + addrT + "_" + stopTime;
//        System.out.println("start query: "+ startQuery);
//        System.out.println("stop query: "+ stopQuery);

        String familyName = "f5";
        String qualifierName = "cl39";

        Scan DBDPhone_T_TSScan = new Scan();
        DBDPhone_T_TSScan.withStartRow(Bytes.toBytes(startQuery),true);
        DBDPhone_T_TSScan.withStopRow(Bytes.toBytes(stopQuery), true);
        ResultScanner resultScannerPhone = DBDPhone_T_TS.getScanner(DBDPhone_T_TSScan);

        Result result;
        List<Map> finalReault = new ArrayList<>();
        while ((result = resultScannerPhone.next()) != null){
            /* 1. to return a list */
            //resultList.add(this.resultFormat(result));
            /* 2. to print all result */
//            System.out.println();
//            System.out.println("------- " + i++ + " --------");
//            queryEtc.printResult(queryEtc.resultFormat(result));
            /* 3. to print address result */
            if (i>3) break;
            Map mapResult = new HashMap();
            System.out.print("Address *** " + i++ +": " );

            List<Cell> cells = result.listCells();
            cells.forEach(
                    cell -> {
                        String familyString = Bytes.toString(CellUtil.cloneFamily(cell));
                        String qualifierString = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String valueString = Bytes.toString(CellUtil.cloneValue(cell));
                        if (familyString.equals(familyName) && qualifierString.equals(qualifierName)) {
                            if (!valueString.equals(" ")) {
                                System.out.print(valueString+"\n");
                                try {
                                    /* 1. when return a list */
                                    //List<String> IDs = QueryAddr_TS_ID(valueString, startTime,stopTime);
                                    /* 2. when print result */
//                                    QueryAddr_TS_ID(valueString, startTime, stopTime);
                                    /* 3. to return a map*/
                                    mapResult.put("address", valueString);
                                    mapResult.put("phone", QueryAddr_TS_ID(valueString, startTime, stopTime));
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            } else System.out.println("\tNo places");
                        }
                    }
            );
            finalReault.add(mapResult);
        }
        return finalReault;
    }


    List<String> QueryAddr_TS_ID(String addr, String startTime, String stopTime) throws IOException {
        int i = 1;
        String startQuery = addr + "_" + startTime + "_/";
        String stopQuery = addr + "_" + stopTime + "_`";
//        System.out.println("start query: "+ startQuery);
//        System.out.println("stop query: "+ stopQuery);

        Scan addr_TS_IDScan = new Scan();
        addr_TS_IDScan.withStartRow(Bytes.toBytes(startQuery), true);
        addr_TS_IDScan.withStopRow(Bytes.toBytes(stopQuery),true);
        ResultScanner resultScannerAddr = Addr_TS_ID.getScanner(addr_TS_IDScan);

        List<String> IDs = new ArrayList<>();
        Result result;
        while ((result = resultScannerAddr.next()) != null){
            String rowkey = Bytes.toString(result.getRow());
            String id = rowkey.split("_")[2];
            if (i++>10) break;

            /*  1. to return a list */
            IDs.add(id);
            /*  2. to print all result */
            //System.out.println("\t  | " + i + " |");
            //queryEtc.printResult(queryEtc.resultFormat(result));
            /*  3. to print id result */
//            System.out.println("\tPhone # " + i + ": " + id);
        }
        /* 1. to return a list */
         return IDs;
    }



}
