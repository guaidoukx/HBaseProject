package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class CompareTest {
    private Table DBD_ID ;
    private Table phoneEnrollInfoDemo;
    private Table phoneEnrollInfo;
    private Table ID_Timestamp;
    private Table Timestamp_ID;
    private Table AssistTable;
    private QueryEtc queryEtc;
    private FileSystem fs;

    public CompareTest() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cloud024");
//        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.master", "10.141.209.224:60000");
        conf.set("fs.defaultFS", "hdfs://10.141.209.224:9000");
        Connection connection = ConnectionFactory.createConnection(conf);
        DBD_ID = connection.getTable(TableName.valueOf("DBD_ID-TS"));
        phoneEnrollInfoDemo = connection.getTable(TableName.valueOf("phoneEnrollInfoDemo"));
        phoneEnrollInfo = connection.getTable(TableName.valueOf("phoneEnrollInfo"));
        ID_Timestamp = connection.getTable(TableName.valueOf("ID-Timestamp"));
        Timestamp_ID = connection.getTable(TableName.valueOf("Timestamp-ID"));
        AssistTable = connection.getTable(TableName.valueOf("AssistTable"));
        queryEtc = new QueryEtc();
        fs = FileSystem.newInstance(conf);
    }


    public void ColumnPrefixFilterTest(Table table, String keyword) throws IOException {
        System.out.println("This is ColumnPrefixFilter");
        Scan scan = new Scan();
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(keyword));
        scan.setFilter(filter);
        Date before = new Date();
        System.out.println("before: " + before.getTime());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        int i = 1;
        while ((result = resultScanner.next()) != null){
            System.out.println("------- " + i++ + " --------");
            queryEtc.printResult(queryEtc.resultFormat(result));
        }
        Date after = new Date();
        System.out.println("after: " + after.getTime());
        long delta = after.getTime() - before.getTime();
        System.out.println("*****" + delta + "*****" + "\n\n");
    }

    public void ColumnRangeFilterTest(Table table, String min, String max) throws IOException {
        System.out.println("This is ColumnRangeFilter");
        Scan scan = new Scan();
        ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes(min), true,  Bytes.toBytes(max), true);
        scan.setFilter(filter);
        Date before = new Date();
        System.out.println("before: " + before.getTime());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        int i = 1;
        while ((result = resultScanner.next()) != null){
            System.out.println("------- " + i++ + " --------");
            queryEtc.printResult(queryEtc.resultFormat(result));
        }
        Date after = new Date();
        System.out.println("after: " + after.getTime());
        long delta = after.getTime() - before.getTime();
        System.out.println("*****" + delta + "*****" + "\n\n");
    }

    public void ColumnValueFilterTest(Table table, String family, String qualifier, String value) throws IOException {
        System.out.println("This is ColumnValueFilter");
        Scan scan = new Scan();
        ColumnValueFilter filter = new ColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                CompareOperator.EQUAL, Bytes.toBytes(value));
        scan.setFilter(filter);
        Date before = new Date();
        System.out.println("before: " + before.getTime());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        int i = 1;
        while ((result = resultScanner.next()) != null){
            System.out.println("------- " + i++ + " --------");
            queryEtc.printResult(queryEtc.resultFormat(result));
        }
        Date after = new Date();
        System.out.println("after: " + after.getTime());
        long delta = after.getTime() - before.getTime();
        System.out.println("*****" + delta + "*****" + "\n\n");
    }

    public List<Long> SingleColumnValueFilterTest(Table table, String family, String qualifier, String value) throws
            IOException {
        System.out.println("This is SingleColumnValueFilter");
        Scan scan = new Scan();
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                CompareOperator.EQUAL, Bytes.toBytes(value));
        scan.setFilter(filter);
        Date before = new Date();
        System.out.println("before: " + before.getTime());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        int i = 1;
        while ((result = resultScanner.next()) != null){
            System.out.println("------- " + i++ + " --------");
            queryEtc.printResult(queryEtc.resultFormat(result));
        }
        Date after = new Date();
        System.out.println("after: " + after.getTime());
        long delta = after.getTime() - before.getTime();
        System.out.println("*****" + delta + "*****" + "\n\n");
        List<Long> results = new ArrayList<>();
        results.add((long) i);
        results.add(delta);
        return results;
    }

    public long PrefixFilterTest(Table table, String keyword) throws IOException {
        System.out.println("This is PrefixFilter");
        Scan scan = new Scan();
        PrefixFilter filter = new PrefixFilter(Bytes.toBytes(keyword));
        scan.setFilter(filter);
        Date before = new Date();
        System.out.println("before: " + before.getTime());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        int i = 1;
        while ((result = resultScanner.next()) != null){
            System.out.println("------- " + i++ + " --------");
//            queryEtc.printResult(queryEtc.resultFormat(result));
        }
        Date after = new Date();
        System.out.println("after: " + after.getTime());
        long delta = after.getTime() - before.getTime();
        System.out.println("*****" + delta + "*****" + "\n\n");
        return delta;
    }

    public long RowFilterTest(Table table, String keyword) throws IOException {
        System.out.println("This is RowFilter RegexStringComparator");
        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^"+keyword));
        scan.setFilter(filter);
        Date before = new Date();
        System.out.println("before: " + before.getTime());
        ResultScanner resultScanner = null;
        try {
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Result result;
        int i = 1;
        while ((result = resultScanner.next()) != null){
            System.out.println("------- " + i++ + " --------");
//            queryEtc.printResult(queryEtc.resultFormat(result));
        }
        Date after = new Date();
        System.out.println("after: " + after.getTime());
        long delta = after.getTime() - before.getTime();
        System.out.println("*****" + delta + "*****" + "\n\n");
        return delta;
    }

    public long RowkeyRangeOneItem(Table table, String item) throws IOException {
        System.out.println("This is RowkeyRangeOneItem startRow & endRow");
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(item+"_"), true);
        scan.withStopRow(Bytes.toBytes(item+"_9"), true);
        Date before = new Date();
        System.out.println("before: " + before.getTime());
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        int i = 1;
        while ((result = resultScanner.next()) != null){
            System.out.println("------- " + i++ + " --------");
//            queryEtc.printResult(queryEtc.resultFormat(result));
        }
        Date after = new Date();
        System.out.println("after: " + after.getTime());
        long delta = after.getTime() - before.getTime();
        System.out.println("*****" + delta + "*****" + "\n\n");
        return delta;
    }

    //    public static void main(String[] args) throws IOException {
//        CompareTest compareTest = new CompareTest();
//        /* test if they can work  */
////        compareTest.ColumnPrefixFilterTest(compareTest.phoneEnrollInfoDemo,"ts");
////        compareTest.PrefixFilterTest(compareTest.phoneEnrollInfoDemo,"22028");
////        compareTest.ColumnRangeFilterTest(compareTest.phoneEnrollInfoDemo,"p", "ts2");
////        compareTest.ColumnValueFilterTest(compareTest.phoneEnrollInfoDemo,"Info", "ts2", "1454858340");
////        compareTest.SingleColumnValueFilterTest(compareTest.phoneEnrollInfoDemo,"Info", "ts2", "1454858340");
////        compareTest.RowFilterTest(compareTest.phoneEnrollInfoDemo,"22028");
//
//        Path filePath = new Path("hdfs://10.141.209.224:9000/test/compareResult.txt");
//        FSDataOutputStream outputStream = compareTest.fs.create(filePath);
//
//
//        /*  PrefixFilter  VS  RowFilter-Regex ^  VS  RowkeyRangeOneItem */
//        List<String> phoneList = new ArrayList<>(Arrays.asList( //
//                "13000002944", "15869956620", "18565124452", "18301739636", "14509607268",
//                "18869941198", "13923170385", "18931777694", "15006367083", "15796649390"));
//        List<String> nameList = new ArrayList<>(Arrays.asList( //
//                "邹喻宋", "奚茅水", "余酆韦", "皮顾赵", "米鲁",
//                "卫郝", "禹平臧", "喻薛柏", "戴贺冯", "俞冯姚"));
//        List<String> IDList = new ArrayList<>(Arrays.asList( //
//                "130223199410221179", "340203197007246194", "441723199011206431", "350681198304063711", "64010019640112885X",
//                "360823200009170046", "450501197812094049", "520327198208071786", "350526196506051795", "330100199608210894"));
//        List<String> emailList = new ArrayList<>(Arrays.asList( //
//                "yhtlszweju@outlook.com", "krecsdli9x@163.com", "vdbumo91gf@outlook.com", "jyonsuadum@126.com", "gb6d145yfg@gmail.com",
//                "d5ic1onftl@163.com", "hjdmb9kffk@163.com", "rlm5h2iuqw@126.com", "tmrexw7p4f@gmail.com", "e7tq1uomhm@hotmail.com"));
//        List<String> addressList = new ArrayList<>(Arrays.asList( //
//                "融安县", "花都区", "平坝县", "尼木县", "西乡塘区",
//                "城厢区", "娄星区", "三水区", "东乌珠穆沁旗", "瑞昌市"));
//        List<String> tsList = new ArrayList<>(Arrays.asList( //
//                "1476398395", "1488979620", "1498035335", "1453803236", "1513958225"));
//
//        /* 对比整个表数据量的大小对查询效率的影响，对比列族中列的大小对查询效率的影响。
//         * 由于查询只涉及到具体的cell，因此，只需要用 SingleColumnValueFilterTest 函数即可 */
//        System.out.println("对比整个表数据量的大小对查询效率的影响，对比列族中列的大小对查询效率的影响。");
//        outputStream.write(Bytes.toBytes("对比整个表数据量的大小对查询效率的影响，对比列族中列的大小对查询效率的影响。\n"));
//        long sumSingleValueInfo6 = 0L; //在phoneInfo表中查询，T6只有10列，查询姓名， family->"info", qualifier->"name"
//        long sumSingleValueID_TS10 = 0L; //在大表中查询（8亿）T10只有10列, 查询email， family->"T10", qualifier->"f9"或"f10"
//        long sumSingleValueID_TS5 = 0L; //在大表中查询（8亿）T5共有55列，查询地级市，family->"T5", qualifier->"f35"
//        long sumSingleValueInfots6 = 0L; //?phoneInfo?????T6??10????ts? family->"Info", qualifier->"ts6"
//
//        for (String name:nameList){
//            List<Long> list = compareTest.SingleColumnValueFilterTest(compareTest.phoneEnrollInfo, "Info", "name", name);
//            sumSingleValueInfo6 += list.get(1);
//            outputStream.write(Bytes.toBytes("there are " + list.get(0) + " items about " + name + ", took " + list.get(1) + "ms.\n"));
//        }System.out.println("name complete.");
//        for (String email:emailList){
//            List<Long> list = compareTest.SingleColumnValueFilterTest(compareTest.ID_Timestamp,"T10","f9", email);
//            sumSingleValueID_TS10 += list.get(1);
//            outputStream.write(Bytes.toBytes("there are " + list.get(0) + " items about " + email + ", took " + list.get(1) + "ms.\n"));
//        }System.out.println("email complete.");
//        for (String address:addressList){
//            List<Long> list = compareTest.SingleColumnValueFilterTest(compareTest.ID_Timestamp,"T5","f35", address);
//            sumSingleValueID_TS5 += list.get(1);
//            outputStream.write(Bytes.toBytes("there are " + list.get(0) + " items about " + address + ", took " + list.get(1) + "ms.\n"));
//        }System.out.println("address complete.");
//        for (String ts:tsList){
//            List<Long> list = compareTest.SingleColumnValueFilterTest(compareTest.phoneEnrollInfo, "Info", "ts2", ts);
//            sumSingleValueInfots6 += list.get(1);
//            outputStream.write(Bytes.toBytes("there are " + list.get(0) + " items about " + ts + ", took " + list.get(1) + "ms.\n"));
//        }System.out.println("name complete.");
//        outputStream.write(Bytes.toBytes("--sumSingleValue Info T6 for name: " + sumSingleValueInfots6 + "ms.\n"));
//        outputStream.write(Bytes.toBytes("--sumSingleValue Info T6 for name: " + sumSingleValueInfo6 + "ms.\n"));
//        outputStream.write(Bytes.toBytes("--sumSingleValue ID_TS T10 for email: " + sumSingleValueID_TS10 + "ms.\n"));
//        outputStream.write(Bytes.toBytes("--sumSingleValue ID_TS T5 for address: " + sumSingleValueID_TS5 + "ms.\n\n"));
//
//
//        /* 测试 rowkeyRange、hbase内置API PrefixFilter 和 使用自定义正则表达式的查询效率
//         * 还有用ID-Timestamp 和 DBD_ID 来测试不同长度的查询条件对同一个查询的影响
//         * 都是使用rowkey作为查询条件（的一部分） */
//        System.out.println("测试 rowkeyRange、hbase内置API PrefixFilter 和 使用自定义正则表达式的查询效率");
//        outputStream.write(Bytes.toBytes("测试 rowkeyRange、hbase内置API PrefixFilter 和 使用自定义正则表达式的查询效率.\n"));
//        long a, sumRowkeyRangeID = 0L; //在大表中查询，只需要表名和ID参数 ID为rowkey
//        long b, sumPrefixID = 0L; //同上
//        long c, sumRowRegexID = 0L; //同上
//        long d, sumRowkeyRangePhone = 0L; //在大表中查询，phone为rowkey
//        long e, sumPrefixPhone = 0L; //同上
//
//        for (String ID:IDList){
//            a = compareTest.RowkeyRangeOneItem(compareTest.ID_Timestamp, ID);
//            outputStream.write(Bytes.toBytes("RowkeyRangeID 使用RowkeyRange-ID: " + a + "ms.\n"));
//            sumRowkeyRangeID += a;
//
//            b = compareTest.PrefixFilterTest(compareTest.ID_Timestamp, ID);
//            outputStream.write(Bytes.toBytes("PrefixID 内置函数-ID: " + b + "ms.\n"));
//            sumPrefixID += b;
//
//            c = compareTest.RowFilterTest(compareTest.ID_Timestamp, ID);
//            outputStream.write(Bytes.toBytes("RowRegexID 自己写的正则表达式-ID: " + c + "ms.\n"));
//            sumRowRegexID += c;
//        }System.out.println("ID complete.");
//        for (String phone:phoneList){
//            d = compareTest.RowkeyRangeOneItem(compareTest.DBD_ID, phone);
//            outputStream.write(Bytes.toBytes("RowkeyRangePhone 使用RowkeyRange-Phone: " + d + "ms.\n"));
//            sumRowkeyRangePhone += d;
//
//            e = compareTest.RowFilterTest(compareTest.DBD_ID, phone);
//            outputStream.write(Bytes.toBytes("PrefixPhone 内置函数-Phone: " + e + "ms.\n"));
//            sumPrefixPhone += e;
//        }System.out.println("phone complete");
//        outputStream.write(Bytes.toBytes("--sumRowkeyRangeID 使用RowkeyRange-ID: " + sumRowkeyRangeID + "ms.\n"));
//        outputStream.write(Bytes.toBytes("--sumPrefixID 内置函数-ID: " + sumPrefixID + "ms.\n"));
//        outputStream.write(Bytes.toBytes("--sumRowRegexID 自己写的正则表达式-ID: " + sumRowRegexID + "ms.\n"));
//        outputStream.write(Bytes.toBytes("--sumRowkeyRangePhone 使用RowkeyRange-Phone: " + sumRowkeyRangePhone + "ms" + ".\n"));
//        outputStream.write(Bytes.toBytes("--sumPrefixPhone 内置函数-Phone: " + sumPrefixPhone + "ms.\n"));
//
//        outputStream.close();
//        compareTest.fs.close();
//    }
    public static void main(String[] args) throws IOException {
        CompareTest compareTest = new CompareTest();
        /* test if they can work  */
        compareTest.ColumnPrefixFilterTest(compareTest.phoneEnrollInfoDemo,"ts");
        compareTest.PrefixFilterTest(compareTest.phoneEnrollInfoDemo,"22028");
        compareTest.ColumnRangeFilterTest(compareTest.phoneEnrollInfoDemo,"p", "ts2");
        compareTest.ColumnValueFilterTest(compareTest.phoneEnrollInfoDemo,"Info", "ts2", "1454858340");
        compareTest.SingleColumnValueFilterTest(compareTest.phoneEnrollInfoDemo,"Info", "ts2", "1454858340");
        compareTest.RowFilterTest(compareTest.phoneEnrollInfoDemo,"22028");
    }
}
