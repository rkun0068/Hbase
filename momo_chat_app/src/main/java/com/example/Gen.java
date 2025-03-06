package com.example;
import com.example.tool.ExcelReader;
import java.util.Map;
import java.util.List;

import com.example.entity.Msg;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
public class Gen {
   public static void main(String[] args) throws Exception{
    Map<String,List<String>> resultMap = ExcelReader.readXlsx("momo_chat_app/src/resources/test_dataset.xlsx","陌陌数据");

    Configuration conf = HBaseConfiguration.create();
    Connection conn = ConnectionFactory.createConnection(conf);

    String TABLE_NAME = "MOMO_CHAT:MSG";
    Table momoChatTable = conn.getTable(TableName.valueOf(TABLE_NAME));

    String cf_name = "C1";
    String col_msg_time = "msg_time";
    String col_sender_nickyname = "sender_nickyname";
    String col_sender_account = "sender_account";
    String col_sender_sex = "sender_sex";
    String col_sender_ip = "sender_ip";
    String col_sender_os = "sender_os";
    String col_sender_phone_type = "sender_phone_type";
    String col_sender_network = "sender_network";
    String col_sender_gps = "sender_gps";
    String col_receiver_nickyname = "receiver_nickyname";
    String col_receiver_ip = "receiver_ip";
    String col_receiver_account = "receiver_account";
    String col_receiver_os = "receiver_os";
    String col_receiver_phone_type = "receiver_phone_type";
    String col_receiver_network = "receiver_network";
    String col_receiver_gps = "receiver_gps";
    String col_receiver_sex = "receiver_sex";
    String col_msg_type = "msg_type";
    String col_distance = "distance";
    String col_message = "message";

    int i = 0;
    int max = 100000;
    while(i < max) {
        Msg msg = ExcelReader.getOneMsg(resultMap);
        Put put = new Put(ExcelReader.getRowKey(msg));

        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_msg_time), Bytes.toBytes(msg.getMsg_time()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_sender_nickyname), Bytes.toBytes(msg.getSender_nickyname()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_sender_account), Bytes.toBytes(msg.getSender_account()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_sender_sex), Bytes.toBytes(msg.getSender_sex()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_sender_ip), Bytes.toBytes(msg.getSender_ip()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_sender_os), Bytes.toBytes(msg.getSender_os()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_sender_phone_type), Bytes.toBytes(msg.getSender_phone_type()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_sender_network), Bytes.toBytes(msg.getSender_network()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_sender_gps), Bytes.toBytes(msg.getSender_gps()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_receiver_nickyname), Bytes.toBytes(msg.getReceiver_nickyname()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_receiver_ip), Bytes.toBytes(msg.getReceiver_ip()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_receiver_account), Bytes.toBytes(msg.getReceiver_account()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_receiver_os), Bytes.toBytes(msg.getReceiver_os()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_receiver_phone_type), Bytes.toBytes(msg.getReceiver_phone_type()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_receiver_network), Bytes.toBytes(msg.getReceiver_network()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_receiver_gps), Bytes.toBytes(msg.getReceiver_gps()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_receiver_sex), Bytes.toBytes(msg.getReceiver_sex()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_msg_type), Bytes.toBytes(msg.getMsg_type()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_distance), Bytes.toBytes(msg.getDistance()));
        put.addColumn(Bytes.toBytes(cf_name), Bytes.toBytes(col_message), Bytes.toBytes(msg.getMessage()));

        // Execute put requests
        momoChatTable.put(put);
        System.out.println(i + " / " + max);
        ++i;
    }

    // close connections
    momoChatTable.close();  
    conn.close();
   } 
}
