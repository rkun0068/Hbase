package com.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter; 
import org.apache.hadoop.hbase.filter.FilterList;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
public class ScanFilterTest {
    private Configuration conf;
    private Connection conn;
    @Before
    public void before() throws IOException {
     conf = HBaseConfiguration.create();
     conn = ConnectionFactory.createConnection(conf);
    }
    
    @After
    public void after() throws IOException {
     conn.close();
    }
    // 查询2020年6月份所有用户的用水量
    @Test
    public void queryTest1() throws IOException{
        Table waterBillTable = conn.getTable(TableName.valueOf("WATER_BILL"));

        Scan scan = new Scan();
        SingleColumnValueFilter startDateFilter = new SingleColumnValueFilter(Bytes.toBytes("C1")
        , Bytes.toBytes("RECORD_DATE")
        , CompareOperator.GREATER_OR_EQUAL
        , Bytes.toBytes("2020-06-01"));

        SingleColumnValueFilter endDateFilter = new SingleColumnValueFilter(Bytes.toBytes("C1")
        , Bytes.toBytes("RECORD_DATE")
        , CompareOperator.LESS_OR_EQUAL
        , Bytes.toBytes("2020-06-30"));

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,startDateFilter,endDateFilter);
        scan.setFilter(filterList);

        ResultScanner resultScanner = waterBillTable.getScanner(scan);
        for(Result result:resultScanner){
            System.out.println("rowkey=>"+Bytes.toString(result.getRow()));
            System.out.println("-----");
            List<Cell>cells = result.listCells();
            for(Cell cell:cells){
                System.out.println(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));
                System.out.println("=>"+Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));
            }
            System.out.println("-----");
        }
        resultScanner.close();
        waterBillTable.close();




    }
}
