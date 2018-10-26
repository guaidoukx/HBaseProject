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
import java.util.*;


public class Phone2Phone {
    static Connection connection;
    private FileSystem fileSystem;
    private QueryEtc queryEtc;
    private Table DBDPhone_t_TS;
    private Table AssistTable;
    private Table Addr_TS_ID;
    public Phone2Phone() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "cloud024:60000");
        conf.set("fs.defaultFS", "hdfs://cloud024:9000");
        fileSystem = FileSystem.newInstance(conf);
        connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        queryEtc = new QueryEtc();
        DBDPhone_t_TS = connection.getTable(TableName.valueOf("DBD_ID-TS"));
        AssistTable = connection.getTable(TableName.valueOf("AssistTable"));
        Addr_TS_ID = connection.getTable(TableName.valueOf("Addr_TS_ID"));
    }

    //1477332905
    //1442339515 13000015154_t5_1442339515 cl21 -> 上海市上海市杨浦区 cl39 -> 西藏那曲地区申扎县
    //1506627160 13000015154_t5_1506627160 cl21 -> 山东省临沂市沂水县 cl39 -> 台湾省桃园市其他
    public static void main(String[] args) throws IOException {
        Phone2Phone phoneAdrrPhone = new Phone2Phone();
        String startTime = "1302339515";
        String stopTime = "1606627160";
        List<String> phoneLisrt1 = phoneAdrrPhone.QueryInPhone("18301739636", startTime, stopTime);
        Map<String, List<String>> phoneList2 = phoneAdrrPhone.QureyInList(phoneLisrt1, startTime,stopTime);
        System.out.println(phoneList2);
//        List<List<List<String>>> phoneList3 = phoneAdrrPhone.QureyInStep3(phoneList2, startTime,stopTime);
//        System.out.println(phoneList3);
//        phoneAdrrPhone.QueryAddr_TS_ID("上海市上海市其他", "1420725593", "1420725594");

    }


//    List<String> QueryDBDPhone_T_TS(String phone, String startTime, String stopTime) throws IOException {
    List<String> QueryInPhone(String phone, String startTime, String stopTime) throws IOException {
        System.out.println();
        System.out.println("Phone: " + phone);
        System.out.println("Start Time: " + startTime);
        System.out.println("Stop Time: " + stopTime );

        int i = 1;
        String tableNum = "_t8";
        String familyName = "f8";
        String qualifierName = "cl38";
        String startQuery = phone + tableNum + "_" + startTime;
        String stopQuery = phone + tableNum + "_" + stopTime;
//        System.out.println("start query: "+ startQuery);
//        System.out.println("stop query: "+ stopQuery);

        Scan DBDPhone_T_TSScan = new Scan();
        DBDPhone_T_TSScan.withStartRow(Bytes.toBytes(startQuery),true);
        DBDPhone_T_TSScan.withStopRow(Bytes.toBytes(stopQuery), true);
        ResultScanner resultScannerPhone = DBDPhone_t_TS.getScanner(DBDPhone_T_TSScan);

        List<String> phoneNums = new ArrayList<>();
        Result result;
        int count = 1;
        while ((result = resultScannerPhone.next()) != null ){
            if (count++ >10) break;
            /* 1. to return a list */
            //resultList.add(this.resultFormat(result));
            /* 2. to print all result */
//            System.out.println();
//            System.out.println("------- " + i++ + " --------");
//            queryEtc.printResult(queryEtc.resultFormat(result));
            /* 3. to print address result */
            System.out.print("Phone *** " + i++ +": " );

            List<Cell> cells = result.listCells();
            cells.forEach(
                    cell -> {
                        String familyString = Bytes.toString(CellUtil.cloneFamily(cell));
                        String qualifierString = Bytes.toString(CellUtil.cloneQualifier(cell));
                        String valueString = Bytes.toString(CellUtil.cloneValue(cell));
                        if (familyString.equals(familyName) && qualifierString.equals(qualifierName)) {
                            if (!valueString.equals(" ")) {
                                System.out.print(valueString+"\n");
                                phoneNums.add(valueString);
                            } else System.out.println("\tNo phones");
                        }
                    }
            );
        }
        if(i == 1) System.out.println("No results");
        return phoneNums;
    }


    Map<String, List<String>> QureyInList(List<String> phoneList1, String startTime,
    String stopTime) throws IOException {
        Map<String, List<String>> result = new HashMap<>();
        for(String phone:phoneList1){
            result.put(phone, QueryInPhone(phone, startTime,stopTime));
        }
        return result;
    }





}
