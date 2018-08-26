package cluster;

import org.apache.hadoop.conf.Configuration;
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
        System.out.println(conf.get("hbase.zookeeper.quorum"));
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

    public long SingleColumnValueFilterTest(Table table, String family, String qualifier, String value) throws
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
        return delta;
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
            queryEtc.printResult(queryEtc.resultFormat(result));
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

    public static void main(String[] args) throws IOException {
        CompareTest compareTest = new CompareTest();
        /* test if they can work  */
//        compareTest.ColumnPrefixFilterTest(compareTest.phoneEnrollInfoDemo,"ts");
//        compareTest.PrefixFilterTest(compareTest.phoneEnrollInfoDemo,"22028");
//        compareTest.ColumnRangeFilterTest(compareTest.phoneEnrollInfoDemo,"p", "ts2");
//        compareTest.ColumnValueFilterTest(compareTest.phoneEnrollInfoDemo,"Info", "ts2", "1454858340");
//        compareTest.SingleColumnValueFilterTest(compareTest.phoneEnrollInfoDemo,"Info", "ts2", "1454858340");
//        compareTest.RowFilterTest(compareTest.phoneEnrollInfoDemo,"22028");

        /*  PrefixFilter  VS  RowFilter-Regex ^  VS  RowkeyRangeOneItem */
        List<String> phoneList = new ArrayList<>(Arrays.asList( //
                "13000002944", "15869956620", "18565124452", "18301739636", "14509607268",
                "18869941198", "13923170385", "18931777694", "15006367083", "15796649390"));
        List<String> nameList = new ArrayList<>(Arrays.asList( //
                "邹喻宋", "奚茅水", "余酆韦", "皮顾赵", "米鲁",
                "卫郝", "禹平臧", "喻薛柏", "戴贺冯", "俞冯姚"));
        List<String> IDList = new ArrayList<>(Arrays.asList( //
                "130223199410221179", "340203197007246194", "441723199011206431", "350681198304063711", "64010019640112885X",
                "360823200009170046", "450501197812094049", "520327198208071786", "350526196506051795", "330100199608210894"));
        List<String> emailList = new ArrayList<>(Arrays.asList( //
                "yhtlszweju@outlook.com", "krecsdli9x@163.com", "vdbumo91gf@outlook.com", "jyonsuadum@126.com", "gb6d145yfg@gmail.com",
                "d5ic1onftl@163.com", "hjdmb9kffk@163.com", "rlm5h2iuqw@126.com", "tmrexw7p4f@gmail.com", "e7tq1uomhm@hotmail.com"));
        List<String> addressList = new ArrayList<>(Arrays.asList( //
                "融安县", "花都区", "平坝县", "尼木县", "西乡塘区",
                "城厢区", "娄星区", "三水区", "东乌珠穆沁旗", "瑞昌市"));


        /* 对比整个表数据量的大小对查询效率的影响，对比列族中列的大小对查询效率的影响。
         * 由于查询只涉及到具体的cell，因此，只需要用 SingleColumnValueFilterTest 函数即可 */
        System.out.println("对比整个表数据量的大小对查询效率的影响，对比列族中列的大小对查询效率的影响。");
        long sumSingleValueInfo6 = 0L; //在phoneInfo表中查询，T6只有10列，查询姓名， family->"info", qualifier->"name"
        long sumSingleValueID_TS10 = 0L; //在大表中查询（8亿）T10只有10列, 查询email， family->"T10", qualifier->"f9"或"f10"
        long sumSingleValueID_TS5 = 0L; //在大表中查询（8亿）T5共有55列，查询地级市，family->"T5", qualifier->"f35"
        for (String name:nameList){
            sumSingleValueInfo6 += compareTest.SingleColumnValueFilterTest(compareTest.phoneEnrollInfo,"info","name",name);
        }for (String email:emailList){
            sumSingleValueID_TS10 += compareTest.SingleColumnValueFilterTest(compareTest.ID_Timestamp,"T10","f9",email);
        }for (String address:addressList){
            sumSingleValueID_TS5 += compareTest.SingleColumnValueFilterTest(compareTest.ID_Timestamp,"T5","f35", address);
        }
        System.out.println("sumSingleValue Info T6 for name: " + sumSingleValueInfo6);
        System.out.println("sumSingleValue ID_TS T10 for email: " + sumSingleValueID_TS10);
        System.out.println("sumSingleValue ID_TS T5 for address: " + sumSingleValueID_TS5);


        /* 测试 rowkeyRange、hbase内置API PrefixFilter 和 使用自定义正则表达式的查询效率
         * 还有用ID-Timestamp 和 DBD_ID 来测试不同长度的查询条件对同一个查询的影响
         * 都是使用rowkey作为查询条件（的一部分） */
        System.out.println("测试 rowkeyRange、hbase内置API PrefixFilter 和 使用自定义正则表达式的查询效率");
        long sumRowkeyRangeID = 0L; //在大表中查询，只需要表名和ID参数 ID为rowkey
        long sumPrefixID = 0L; //同上
        long sumRowRegexID = 0L; //同上
        long sumRowkeyRangePhone = 0L; //在大表中查询，phone为rowkey
        long sumPrefixPhone = 0L; //同上
        for (String ID:IDList){
            sumRowkeyRangeID += compareTest.RowkeyRangeOneItem(compareTest.ID_Timestamp, ID);
            sumPrefixID += compareTest.PrefixFilterTest(compareTest.ID_Timestamp, ID);
            sumRowRegexID += compareTest.RowFilterTest(compareTest.ID_Timestamp, ID);
        }for (String phone:phoneList){
            sumRowkeyRangePhone += compareTest.RowkeyRangeOneItem(compareTest.DBD_ID, phone);
            sumPrefixPhone += compareTest.RowFilterTest(compareTest.DBD_ID, phone);
        }
        System.out.println("sumRowkeyRangeID 使用RowkeyRange-ID: " + sumRowkeyRangeID);
        System.out.println("sumPrefixID 内置函数-ID: " + sumPrefixID);
        System.out.println("sumRowRegexID 自己写的正则表达式-ID: " + sumRowRegexID);
        System.out.println("sumRowkeyRangePhone 使用RowkeyRange-Phone: " + sumRowkeyRangePhone);
        System.out.println("sumPrefixPhone 内置函数-Phone: " + sumPrefixPhone );


        /* 对比使用  */

//        long sumPrefixFilter = 0L;
//        long sumRowkeyRange = 0L;
//        long sumRowFilter = 0L;
//        for (String query:phoneList){
//            sumRowkeyRange += compareTest.RowkeyRangeOneItem(compareTest.DBD_ID, query+"_t", query+"_t9");
//            sumPrefixFilter += compareTest.PrefixFilterTest(compareTest.DBD_ID,query+"_t");
//            sumRowFilter += compareTest.RowFilterTest(compareTest.DBD_ID, query);
//        }
//        System.out.println("sumPrefixFilter " + sumPrefixFilter);
//        System.out.println("sumRowkeyRange " + sumRowkeyRange);
//        System.out.println("sumRowFilter " + sumRowFilter);



//        compareTest.RowFilterTest(compareTest.DBD_ID,"13000002944_t");
//        compareTest.SingleColumnValueFilterTest(compareTest.phoneEnrollInfo,"Info", "name", "米鲁");



    }


}
