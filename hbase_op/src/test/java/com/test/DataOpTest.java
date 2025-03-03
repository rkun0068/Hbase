package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.After;
import org.junit.Before;
import java.io.IOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Delete;

public class DataOpTest {
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
   
// 1.使用Hbase连接获取Htable
// 2.构建ROWKEY、列蔟名、列名
// 3.构建Put对象（对应put命令）
// 4.添加姓名列
// 5.使用Htable表对象执行put操作
// 6.关闭Htable表对象
@Test
public void addTest() throws IOException {
    // 1.使用Hbase连接获取Htable
    TableName waterBillTableName = TableName.valueOf("WATER_BILL");
    Table waterBillTable = conn.getTable(waterBillTableName);

    // 2.构建ROWKEY、列蔟名、列名
    String rowkey = "4944191";
    String cfName = "C1";
    String colName = "NAME";

    // 3.构建Put对象（对应put命令）
    Put put = new Put(Bytes.toBytes(rowkey));

    // 4.添加姓名列
    put.addColumn(Bytes.toBytes(cfName)
        , Bytes.toBytes(colName)
        , Bytes.toBytes("登卫红"));

    // 5.使用Htable表对象执行put操作
    waterBillTable.put(put);
    // 6. 关闭表
    waterBillTable.close();
}

// 插入其他列
@Test
public void addMutiTest() throws IOException {
    TableName waterBillTableName = TableName.valueOf("WATER_BILL");
    Table waterBillTable = conn.getTable(waterBillTableName);
        // 2.构建ROWKEY、列蔟名、列名
    String rowkey = "4944191";
    String cfName = "C1";
    String colName = "NAME";
    String colADDRESS = "ADDRESS";
    String colSEX = "SEX";
    String colPAY_DATE = "PAY_DATE";
    String colNUM_CURRENT = "NUM_CURRENT";
    String colNUM_PREVIOUS = "NUM_PREVIOUS";
    String colNUM_USAGE = "NUM_USAGE";
    String colTOTAL_MONEY = "TOTAL_MONEY";
    String colRECORD_DATE = "RECORD_DATE";
    String colLATEST_DATE = "LATEST_DATE";
    
        // 3.构建Put对象（对应put命令）
    Put put = new Put(Bytes.toBytes(rowkey));
    
        // 4.添加姓名列
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colName)
                , Bytes.toBytes("登卫红"));
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colADDRESS)
                , Bytes.toBytes("贵州省铜仁市德江县7单元267室"));
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colSEX)
                , Bytes.toBytes("男"));
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colPAY_DATE)
                , Bytes.toBytes("2020-05-10"));
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colNUM_CURRENT)
                , Bytes.toBytes("308.1"));
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colNUM_PREVIOUS)
                , Bytes.toBytes("283.1"));
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colNUM_USAGE)
                , Bytes.toBytes("25"));
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colTOTAL_MONEY)
                , Bytes.toBytes("150"));
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colRECORD_DATE)
                , Bytes.toBytes("2020-04-25"));
    put.addColumn(Bytes.toBytes(cfName)
                , Bytes.toBytes(colLATEST_DATE)
                , Bytes.toBytes("2020-06-09"));
    
        // 5.使用Htable表对象执行put操作
    waterBillTable.put(put);
    
        // 6. 关闭表
    waterBillTable.close();
}
// Search
// 1.获取HTable
// 2.使用rowkey构建Get对象
// 3.执行get请求
// 4.获取所有单元格
// 5.打印rowkey
// 6.迭代单元格列表
// 7.关闭表

@Test
public void getOneTest() throws IOException{
    TableName waterBillTableName = TableName.valueOf("WATER_BILL");
    Table waterBillTable = conn.getTable(waterBillTableName);

    Get get = new Get(Bytes.toBytes("4944191"));
    Result result = waterBillTable.get(get);
    List<Cell>cells = result.listCells();
    System.out.println("rowkey=>"+Bytes.toString(result.getRow()));
    for(Cell cell:cells){
        System.out.print(Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));
        System.out.println("=>"+Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()));

    }
    waterBillTable.close();
}

// 删除rowkey为4944191的整条数据。

// 实现步骤：
// 1.获取HTable对象
// 2.根据rowkey构建delete对象
// 3.执行delete请求
// 4.关闭表
@Test
public void deleteOneTest() throws IOException{
    TableName waterBillTableName = TableName.valueOf("WATER_BILL");
    Table waterBillTable = conn.getTable(waterBillTableName);

    Delete delete = new Delete(Bytes.toBytes("4944191"));
    waterBillTable.delete(delete);
    waterBillTable.close();
   

}
}
