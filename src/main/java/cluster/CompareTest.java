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
//        conf.set("hbase.zookeeper.quorum", "10.141.209.224, 10.141.209.223, 10.141.209.222");
        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "10.141.209.224:60000");
        conf.set("fs.defaultFS", "hdfs://10.141.209.224:9000");
        Connection connection = ConnectionFactory.createConnection(conf);
        DBD_ID = connection.getTable(TableName.valueOf("DBD_ID-TS"));
        phoneEnrollInfoDemo = connection.getTable(TableName.valueOf("phoneEnrollInfoDemo"));
        phoneEnrollInfo = connection.getTable(TableName.valueOf("phoneEnrollInfo"));
        ID_Timestamp = connection.getTable(TableName.valueOf("ID-Timestamp"));
        Timestamp_ID = connection.getTable(TableName.valueOf("Timestamp-ID"));
        AssistTable = connection.getTable(TableName.valueOf("AssistTable"));
        queryEtc = new QueryEtc();
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

    public void SingleColumnValueFilterTest(Table table, String family, String qualifier, String value) throws
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
    }

    public long RowFilterTest(Table table, String keyword) throws IOException {
        System.out.println("This is RowFilter RegexStringComparator");
        Scan scan = new Scan();
        Filter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^"+keyword));
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


    public long RowkeyRange(Table table, String min, String max) throws IOException {
        System.out.println("This is RowkeyRange startRow & endRow");
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(min), true);
        scan.withStopRow(Bytes.toBytes(max), true);
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

        /*  PrefixFilter  VS  RowFilter-Regex ^  VS  RowkeyRange */
        List<String> phoneList = new ArrayList<>(Arrays.asList( //
                "13000002944", "15869956620", "18565124452", "18301739636", "14509607268",
                "18869941198", "13923170385", "18931777694", "15006367083", "15796649390"));

        List<String> nameList = new ArrayList<>(Arrays.asList(
                "邹喻宋", "奚茅水", "余酆韦", "皮顾赵", "米鲁",
                "卫郝", "禹平臧", "喻薛柏", "戴贺冯", "俞冯姚"));

        long sumPrefixFilter = 0L;
        long sumRowkeyRange = 0L;
        long sumRowFilter = 0L;
//        for (String query:phoneList){
//            sumRowkeyRange += compareTest.RowkeyRange(compareTest.DBD_ID, query+"_t", query+"_t9");
//            sumPrefixFilter += compareTest.PrefixFilterTest(compareTest.DBD_ID,query+"_t");
//            sumRowFilter += compareTest.RowFilterTest(compareTest.DBD_ID, query);
//        }
//        System.out.println("sumPrefixFilter " + sumPrefixFilter);
//        System.out.println("sumRowkeyRange " + sumRowkeyRange);
//        System.out.println("sumRowFilter " + sumRowFilter);
        compareTest.RowFilterTest(compareTest.DBD_ID,"13000002944_t");

//        compareTest.SingleColumnValueFilterTest(compareTest.phoneEnrollInfo,"Info", "name", "米鲁");



    }


}
