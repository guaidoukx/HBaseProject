package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class Query {
    private static Connection connection;
    private Table phoneEnrollInfo;
    private Table DBD_ID;
    private Table AssistTable;
    private Table ID_Timestamp;

    public Query() throws IOException {
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

    public List<Map<String, Map<String, Map<String, String>>>> queryByMobile(String phone) throws IOException {
        List<Map<String, Map<String, Map<String, String>>>> resultList = new ArrayList<>();
        int i = 1;
        Scan scanDBD_ID = new Scan();
        scanDBD_ID.withStartRow(Bytes.toBytes(phone + "_t"), true);
        scanDBD_ID.withStopRow(Bytes.toBytes(phone + "_t9"), true);
        ResultScanner resultScannerFilterList = DBD_ID.getScanner(scanDBD_ID);

        Get getID = new Get(Bytes.toBytes(phone));
        Result resultID = AssistTable.get(getID);
        if (resultID.size() != 0) {
            byte[] ID = resultID.value();

            Get getAll = new Get(ID);
            Result resultAll = phoneEnrollInfo.get(getAll);
            if (resultAll.size() != 0) {
//                System.out.println("------- " + i++ + " --------");
                /* to return a list */
                resultList.add(this.resultFormat(resultAll));
                /* to print result */
//                this.printResult(this.resultFormat(resultAll));
            } else {
                System.out.println("No ID matches the phone number " + phone);
            }
        } else {
            System.out.println("There are no records about the phone number " + phone);
        }

        Result result;
        while ((result = resultScannerFilterList.next()) != null) {
//            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            resultList.add(this.resultFormat(result));
            /* to print result */
//            this.printResult(this.resultFormat(result));
        }
//        System.out.println("queryInRowkeyRange: There is no more row in this range! ");
        resultScannerFilterList.close();

        return resultList;
    }

    public List<Map<String, Map<String, Map<String, String>>>> queryByID(String ID) throws IOException {
        List<Map<String, Map<String, Map<String, String>>>> resultList = new ArrayList<>();
        int i = 1;

        Get getAll = new Get(Bytes.toBytes(ID));
        Result resultAll = phoneEnrollInfo.get(getAll);
        if (resultAll.size() != 0) {
//            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            resultList.add(this.resultFormat(resultAll));
            /* to print result */
//            this.printResult(this.resultFormat(resultAll));

            //some changes
            List<Cell> CellList = resultAll.getColumnCells(Bytes.toBytes("Info"), Bytes.toBytes("phoneNum"));
            String phone = Bytes.toString(CellUtil.cloneValue(CellList.get(0)));
//        System.out.println(phone);

            Scan scanDBD_ID = new Scan();
            scanDBD_ID.withStartRow(Bytes.toBytes(phone+"_t"), true);
            scanDBD_ID.withStopRow(Bytes.toBytes(phone+"_t9"), true);
            ResultScanner resultScannerFilterList = DBD_ID.getScanner(scanDBD_ID);

            Result result;
            while ((result = resultScannerFilterList.next()) != null) {
//            System.out.println("------- " + i++ + " --------");
                /* to return a list */
                resultList.add(this.resultFormat(result));
                /* to print result */
//            this.printResult(this.resultFormat(result));
            }
//        System.out.println("queryInRowkeyRange: There is no more row in this range! ");
            resultScannerFilterList.close();
        } else {
//            System.out.println("No ID matches the ID " + ID);
        }

        return resultList;
    }

    /**
     * 通过身份号查询，使用的是新的数据表 rowKey 设计为 id_ts
     * 由于数据表结构变化，数据解析函数尚未完成
     *
     * @param ID 身份证号
     * @return List, structure like the following json object
     *         [{"411627198611171340":{"Info":{"ts2":"1465673841","ts1":"1485685957","ts4":"1489230188","ts3":"1431762115","ts6":"1471096070","ts5":"1468375317","name":"雷任禹","phoneNum":"13193613316","phoneState":"销号"}}},{"411627198611171340-1444586318":{"T5":{"f21":"安徽省阜阳市其他","f32":"中国","f01":"1444586318","f12":"x点部","f34":"上海市","f33":"上海市","f44":"13193613316","f03":"cny","f14":"中国","f02":"176401086723","f13":"4lrsm","f35":"闸北区","f16":"阜阳市","f15":"安徽省","f26":"4788511","f18":"退回件","f17":"其他","f39":"上海市上海市闸北区","f09":"b类包裹","f19":"tui hui jian"}}},{"411627198611171340-1461400119":{"T8":{"f50":"NULL","f30":"73.94357974","f52":"NULL","f51":"NULL","f32":"3536,159543858","f54":"NULL","f31":"2311","f53":"NULL","f12":"NULL","f34":"NULL","f11":"NULL","f33":"61567","f14":"NULL","f38":"18341836176","f15":"NULL","f41":"56042","f43":"NULL","f42":"75","f01":"1461400119","f22":"90","f44":"NULL","f25":"460064902081654","f47":"NULL","f24":"13193613316","f27":"2160","f26":"90345045781204","f29":"19.5207248","f28":"46"}}},{"411627198611171340-1463543064":{"T5":{"f21":"香港香港沙田区","f32":"中国","f01":"1463543064","f12":"x点部","f34":"宁波市","f33":"浙江省","f44":"13193613316","f03":"cny","f14":"中国","f02":"532796904279","f13":"dnaj4","f35":"奉化市","f16":"香港","f15":"香港","f26":"1985876","f18":"退回件","f17":"沙田区","f39":"浙江省宁波市奉化市","f09":"b类包裹","f19":"tui hui jian"}}},{"411627198611171340-1463757635":{"T7":{"f10":"RASIUS","f21":"13193613316","f20":"MHXZVJVI9BDBYW","f12":"0","f23":"ctnet@mycdma.cn","f11":"156","f22":"113.149.43.186","f14":"3GWZ_ENTER","f25":"26219,165907750","f05":"189.51.76.94","f27":"NULL","f26":"1463757635","f07":"UDP","f18":"WAPRADIUS","f29":"0","f06":"189.51.76.94","f17":"WAPRADIUS","f28":"1457852806","f09":"59098","f08":"6663","f19":"460731569500529"}}},{"411627198611171340-1480272686":{"T2":{"f30":"460","f10":"181.38.30.203","f32":"460","f31":"LTSP0BVNFZMRJI","f12":"TCP","f34":"2704","f11":"191.248.109.137","f33":"13843,186997328","f14":"82347","f36":"hello","f13":"7759","f35":"root","f16":"156","f15":"TELNET","f18":"NDPPRH3QWCZWTRAF","f17":"156","f19":"FILE_TRANS","f43":"65282843800096","f20":"telnet","f42":"Emerson Network Power Co., Ltd. Linux/armv4lt 2.6.24  FSU login: Password: FSUID","f01":"1480272686","f23":"TELNET","f45":"2_88_1420807174_984_16_619104.txt","f22":"TELNET","f44":"CDP_ORIGINAL","f25":"SRC_ACC","f02":"VPN_DEC","f24":"13193613316","f05":"7005_5","f27":"0","f04":"NULL","f26":"61470","f07":"snv9eovkwikx","f29":"SRC_ACC","f06":"qjhavb3enljm","f28":"460030775406959","f09":"ZRCAJYXFGTUSCVXIAW0DZ95K","f08":"ZRCAJYXFGTUSCVXIAW0DZ95K"}}},{"411627198611171340-1484449569":{"T8":{"f50":"NULL","f30":"14.39131185","f52":"NULL","f51":"NULL","f32":"19647,208067969","f54":"NULL","f31":"28175","f53":"NULL","f12":"NULL","f34":"NULL","f11":"NULL","f33":"61877","f14":"NULL","f38":"15264358610","f15":"NULL","f41":"11947","f43":"NULL","f42":"28","f01":"1484449569","f22":"95","f44":"NULL","f25":"460333917225870","f47":"NULL","f24":"13193613316","f27":"8128","f26":"99999001352283","f29":"44.0974036","f28":"42"}}},{"411627198611171340-1509383294":{"T3":{"f50":"SRC_ACC","f52":"SRC_CAP","f96":"1442377902","f10":"O6EVWJKRLNEIFAWMDX3YFMTU;1TVPJAE5B0BMSZEA7OGD8FJX","f54":"SRC_CAP","f97":"NULL","f12":"78.221.21.187","f14":"TCP","f13":"118.118.106.157","f16":"60","f15":"64755","f18":"0","f17":"OTHERS","f19":"156","f61":"NULL","f63":"NULL","f62":"NULL","f21":"IM_AUDIOCHAT","f20":"lmgaapcwpsbm9brj","f67":"wxid_s4wrlehimgabnt","f22":"微信","f25":"IM","f24":"微信","f27":"SRC_ACC","f26":"13193613316","f29":"1","f28":"4748","f30":"2095597581035696","f32":"209","f31":"SRC_ACC","f34":"SRC_ACC","f78":"0","f33":"139365578894018","f77":"CS","f35":"23291,131306867","f79":"wxwpier","f81":"0","f80":"RECV","f40":"JAFB2CLZAH","f01":"1509383294","f45":"7_34_1448830388_377_99_164339.avi","f44":"CDP_DUPLEXAUDIO","f03":"617754429645","f02":"7005_5","f46":"61425587","f05":"GTP;GTP","f04":"e1do537pq9ot","f48":"SRC_CAP","f07":"2.117.10.146;128.161.13.202","f06":"2.117.10.146;128.161.13.202","f09":"O6EVWJKRLNEIFAWMDX3YFMTU;1TVPJAE5B0BMSZEA7OGD8FJX","f08":";"}}}]
     * @throws IOException
     */
    public List<Map<String, Map<String, Map<String, String>>>> queryByIDNew(String ID) throws IOException {
        List<Map<String, Map<String, Map<String, String>>>> resultList = new ArrayList<>();
        int i = 1;
        Scan scanID_TS = new Scan();
        scanID_TS.withStartRow(Bytes.toBytes(ID + "-"), true);
        scanID_TS.withStopRow(Bytes.toBytes(ID + "-9"), true);
        ResultScanner resultScannerFilterList = ID_Timestamp.getScanner(scanID_TS);

        Get getAll = new Get(Bytes.toBytes(ID));
        Result resultAll = phoneEnrollInfo.get(getAll);
        if (resultAll.size() != 0) {
//            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            resultList.add(this.resultFormat(resultAll));
            /* to print result */
//            this.printResult(this.resultFormat(resultAll));
        } else {
//            System.out.println("No ID matches the ID " + ID);
        }

        Result result;
        while ((result = resultScannerFilterList.next()) != null) {
//            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            resultList.add(this.resultFormat(result));
            /* to print result */
//            this.printResult(this.resultFormat(result));
        }
//        System.out.println("queryInRowkeyRange: There is no more row in this range! ");
        resultScannerFilterList.close();

        return resultList;
    }

    /**
     * always used in a query method to get a format result.
     *
     * @param result, No need to check if it is empty or not. This step should be in query method.
     *                In a query method:
     *                1. when there's a result, call for @resultFormat method, return format result -> triple Map.
     *                2. otherwise, query method return @null.
     */
    public Map<String, Map<String, Map<String, String>>> resultFormat(Result result) {
        //
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
     *
     * @param resultMap is either 'Map<String,Map<String, Map<String,String>>>' or @null, so check it first
     */
    public void printResult(Map<String, Map<String, Map<String, String>>> resultMap) {
        if (resultMap == null) {
            System.out.println("printResult: ResultMap is empty! ");
        } else {
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
}
