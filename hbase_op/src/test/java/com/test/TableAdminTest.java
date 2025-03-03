package com.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.io.IOException;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
public class TableAdminTest {
    private Configuration conf;
    private Connection conn;
    private Admin admin;

    @Before
    public void before() throws IOException {
        conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();
    }

    @After
    public void after() throws IOException {
        admin.close();
        conn.close();
    }
    @Test 
    public void createTableTest() throws IOException{
        String TABLE_NAME = "WATER_BILL";

        String COLUMN_FAMILY = "C1";
        if(admin.tableExists(TableName.valueOf(TABLE_NAME))){
            System.out.println("Table already exists");
            return;
        }
        // 创建表描述Builder
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
        // 创建列族描述 Builder
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY));
        // 构建列族描述符
        ColumnFamilyDescriptor columnFamilyDescriptor = columnFamilyDescriptorBuilder.build();
        // 添加列族描述符到表描述符
        tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
        // 构建表描述符
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        // 创建表
        admin.createTable(tableDescriptor);
        System.out.println("Table created successfully");


    }

    // Delete the table
    @Test
    public void dropTable() throws IOException{
        TableName tableName = TableName.valueOf("WATER_BILL");
        if (admin.tableExists(tableName)){
            admin.disableTable(tableName);  
            admin.deleteTable(tableName);
        }
    }

    



    
}
