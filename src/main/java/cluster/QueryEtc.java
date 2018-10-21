package cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class QueryEtc {
    static Connection connection;
    FileSystem fileSystem;

    public QueryEtc() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.141.209.224");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", "cloud024:60000");
        conf.set("fs.defaultFS", "hdfs://cloud024:9000");
        fileSystem = FileSystem.newInstance(conf);
        connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
    }


    public static void main(String[] args) throws IOException {
        QueryEtc queryEtc = new QueryEtc();
        Table phoneEnrollInfoDemo = connection.getTable(TableName.valueOf("phoneEnrollInfoDemo"));
        Table phoneEnrollInfo = connection.getTable(TableName.valueOf("phoneEnrollInfo"));
        Table test1 = connection.getTable(TableName.valueOf("test1"));
        Table DBD_ID = connection.getTable(TableName.valueOf("DBD_ID-TS"));
        Table Addr_TS_ID = connection.getTable(TableName.valueOf("Addr_TS_ID"));

        Date before = new Date();
        queryEtc.queryInRowkeyRange(Addr_TS_ID, "上海市上海市其他_0", "上海市上海市其他_z");
        Date after = new Date();
        System.out.println((after.getTime()-before.getTime()));

        /* test query one in complete rowkey */
//        Map<String,Map<String, Map<String,String>>> resultMap = queryEtc.queryInRowkey(test1,"row1");
//        queryEtc.printResult(resultMap);

//        Map<String,Map<String, Map<String,String>>> resultMap = queryEtc.queryInRowkey(phoneEnrollInfoDemo,
//                "52032719840313179");
//        queryEtc.printResult(resultMap);


        /* test start row & end row  */
//        List<Map<String,Map<String, Map<String,String>>>> resultMapList = queryEtc.queryInRowkeyRange
//                (phoneEnrollInfoDemo,"1",  "520327198403131799");
//        resultMapList.forEach( result -> queryEtc.printResult(result) );
//        queryEtc.queryInRowkeyRange(DBD_ID, "18565124452_t8");
//        queryEtc.queryInRowkeyRange(DBD_ID, "18644883991_t8_0","18644883991_t8_1533093264089" );
//        queryEtc.printResult(queryEtc.queryInRowkey(DBD_ID, "13000373621_t9_1510733005"));

        /* test filter */

    }


    /**
     * use regex to find out result.
     *      1. ^abc : those begin with "abc"
     *      2. abc$ : those end with "abc"
     * not clear about BinaryComparator, what does "LESS" mean?
     * @param keyword just mean keywords
     * @throws IOException I have no idea
     */
    public void filterTest(Table table, String keyword) throws IOException {
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareOperator.LESS, new BinaryComparator(Bytes.toBytes(keyword))));
        scan.setFilter(new PrefixFilter(Bytes.toBytes(keyword)));
        System.out.println("out: ");
        ResultScanner scanner1 = table.getScanner(scan);
        int i = 1;
        for(Result result:scanner1){
            System.out.println("------- "+ i++ + " --------");
            this.printResult(this.resultFormat(result));
        }
        scanner1.close();



//        Filter filter3 = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(keyword));
//        scan.setFilter(filter3);
//        System.out.println("包含子串" + keyword +"的：");
//        ResultScanner scanner3 = table.getScanner(scan);
//        i = 1;
//        for (Result result:scanner3){
//            System.out.println("------- "+ i++ + " --------");
//            this.printResult(this.resultFormat(result));
//        }
//        scanner3.close();
    }


    /**
     * Map to get several filters
     * @param keywords true means former ^; false means later $
     * @param mode true means MUST_PASS_ALL; false means MUST_PASS_ONE
     * @throws IOException have no idea
     */
    public void regexFilter(Table table, Map<String, Boolean> keywords, boolean mode) throws IOException {
        Scan scan = new Scan();
        FilterList filterList ;
        if (mode) {
            filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        }else {
            filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        }
        Set<String> keySet = keywords.keySet();
        for(String key:keySet){
            if(keywords.get(key)){
                filterList.addFilter(new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^"+key)));
            }else {
                filterList.addFilter(new RowFilter(CompareOperator.EQUAL, new RegexStringComparator(key+"$")));
            }
        }
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        int i = 1;
        for (Result result:scanner){
            System.out.println("------- "+ i++ + " --------");
            this.printResult(this.resultFormat(result));
        }
        scanner.close();
    }


    /**
     *
     * @param keyword just only one query, in ^ mode
     * @throws IOException I have no idea
     */
    public void regexFilterOne(Table table, String keyword) throws IOException {
        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^"+keyword)));
        ResultScanner scanner = table.getScanner(scan);
        int i = 1;
        for (Result result:scanner){
            System.out.println("------- "+ i++ + " --------");
            this.printResult(this.resultFormat(result));
        }
        scanner.close();
    }


    public void singleValueFilter(Table table, String cf, String qualifier, String keyword) throws IOException {
        Scan scan = new Scan();
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(cf), Bytes.toBytes(qualifier),
                CompareOperator.EQUAL, Bytes.toBytes(keyword) );
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        Result result;
        int i = 1;
        while ((result=resultScanner.next()) != null) {
            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            //resultList.add(this.resultFormat(result));
            /* to print result */
            printResult(resultFormat(result));
        }
    }


    /**
     * should query in exact rowkey.
     * @param rowkey a query.
     * @return format result: when query in exact & existed rowkey; null: otherwise.
     * @throws IOException I have no idea.
     */
    public Map<String,Map<String, Map<String,String>>> queryInRowkey(Table table, String rowkey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        if (result.size() != 0) {
            return this.resultFormat(result);
        }else {
            System.out.println("queryInRowkey: There is no rowkey " + rowkey);
            return null;
        }
    }


    /**
     * @param startRow no need in exact rowkey, always exact in front of the rowkey, included in result
     * @param endRow same as startRow
     * @return return a list of format result:
     *         1. the list is empty when there is no result (startRow > endRow)
     *         2. the list is not empty otherwise
     * @throws IOException I have no idea.
     */
    public List<Map<String,Map<String, Map<String,String>>>> queryInRowkeyRange(Table table, String startRow, String endRow)
            throws IOException {
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow), true);
        scan.withStopRow(Bytes.toBytes(endRow), true);
        ResultScanner resultScannerFilterList = table.getScanner(scan);
        List<Map<String,Map<String, Map<String,String>>>> resultList = new ArrayList<>();
        int i = 1;
        Result result;
        while ((result = resultScannerFilterList.next()) != null){
            System.out.println("------- " + i++ + " --------");
            /* to return a list */
            //resultList.add(this.resultFormat(result));
            /* to print result */
            printResult(resultFormat(result));
        }
        System.out.println("queryInRowkeyRange: There is no row in this range! ");
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






}
