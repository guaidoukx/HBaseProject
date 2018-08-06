package cluster;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.*;

public class CompareTest {



    /* startRow-endRow VS regex mode */
    public void One() throws IOException {
        QueryEtc queryEtc = new QueryEtc();
        Table DBD_ID = QueryEtc.connection.getTable(TableName.valueOf("DBD_ID-TS"));
        Table phoneEnrollInfoDemo = QueryEtc.connection.getTable(TableName.valueOf("phoneEnrollInfoDemo"));

        List<String> queryList = new ArrayList<>(Arrays.asList( //
                "13000002944", "15869956620", "18565124452", "18301739636", "14509607268",
                "18869941198", "13923170385", "18931777694", "15006367083", "15796649390"));

        Date Start = new Date();
        long start = Start.getTime();
        System.out.println("start: " + start);

        for (String query:queryList) {
            queryEtc.queryInRowkeyRange(DBD_ID, query+"_t" , query+"_t9" );
        }
        Date Middle = new Date();
        long middle = Middle.getTime();
        System.out.println("Query in startRow-endRow mode: " + (middle-start));

        for (String query:queryList) {
            queryEtc.regexFilterOne(DBD_ID, query+"_t");
        }
        Date End = new Date();
        long end = End.getTime();
        System.out.println("Query in regex mode: " + (middle-end));
    }







    public static void main(String[] args) throws IOException {
        CompareTest compareTest = new CompareTest();
        compareTest.One();
    }


}
