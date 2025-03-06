package com.example.service;
import java.util.List;
import com.example.entity.Msg;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Connection;
import java.text.SimpleDateFormat;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.Filter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.Cell;
public class HbaseNativeChatMessageService implements ChatMessageService {
    // Hbase connection
    private Connection connection;
    // date formater
    private SimpleDateFormat sdf;

    private Table tableMsg;

    private ExecutorService executorServiceMsg;
    public HbaseNativeChatMessageService() {
        try {
            Configuration cfg = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(cfg);
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            
            executorServiceMsg = Executors.newFixedThreadPool(2);
            tableMsg = connection.getTable(TableName.valueOf("MOMO_CHAT:MSG"), executorServiceMsg);
        } catch (IOException e) {
            System.out.println("**Get HBase connection failed**");
            throw new RuntimeException(e);
        }
    }
    /*
     * Get messages based on date, sender, receiver
     * @param date
     * @param sender
     * @param receiver
     * @return
     * @throws Exception
     */
    @Override
    public List<Msg> getMessage(String date,String sender,String receiver) throws Exception {
        if(connection == null){
            throw new Exception("HBase connection is not initialized");
        }
        Scan scan = new Scan();
        // build time range
        String startDate = date + " 00:00:00";
        String endDate = date + " 23:59:59";
        SingleColumnValueFilter startFilter = new SingleColumnValueFilter(Bytes.toBytes("C1"),
        Bytes.toBytes("msg_time"),CompareOperator.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes(startDate+"")));
        
        SingleColumnValueFilter endFilter = new SingleColumnValueFilter(Bytes.toBytes("C1"),
        Bytes.toBytes("msg_time"),CompareOperator.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes(endDate+"")));

        // build sneder filter
        SingleColumnValueFilter senderFilter = new SingleColumnValueFilter(Bytes.toBytes("C1"),
        Bytes.toBytes("sender_account"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes(sender)));

        // build receiver filter
        SingleColumnValueFilter receiverFilter = new SingleColumnValueFilter(Bytes.toBytes("C1"),
        Bytes.toBytes("receiver_account"),CompareOperator.EQUAL,new BinaryComparator(Bytes.toBytes(receiver)));

        // build filter list
        Filter filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL
        ,startFilter,endFilter,senderFilter,receiverFilter);

        scan.setFilter(filterList);
        ResultScanner scanner = tableMsg.getScanner(scan);
        Iterator<Result> iter =scanner.iterator();
        List<Msg> msgList = new ArrayList<>();
        while(iter.hasNext()){
            Result result = iter.next();
            Msg msg = new Msg();
            while(result.advance()){
                Cell cell = result.current();
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());

                if(columnName.equalsIgnoreCase("msg_time")){
                    msg.setMsg_time(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("sender_nickyname")){
                    msg.setSender_nickyname(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("sender_account")){
                    msg.setSender_account(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("sender_sex")){
                    msg.setSender_sex(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("sender_ip")){
                    msg.setSender_ip(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("sender_os")){
                    msg.setSender_os(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("sender_phone_type")){
                    msg.setSender_phone_type(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("sender_network")){
                    msg.setSender_network(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("sender_gps")){
                    msg.setSender_gps(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("receiver_nickyname")){
                    msg.setReceiver_nickyname(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("receiver_ip")){
                    msg.setReceiver_ip(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("receiver_account")){
                    msg.setReceiver_account(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("receiver_os")){
                    msg.setReceiver_os(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("receiver_phone_type")){
                    msg.setReceiver_phone_type(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("receiver_network")){
                    msg.setReceiver_network(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("receiver_gps")){
                    msg.setReceiver_gps(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("receiver_sex")){
                    msg.setReceiver_sex(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("msg_type")){
                    msg.setMsg_type(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("distance")){
                    msg.setDistance(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }
                if(columnName.equalsIgnoreCase("message")){
                    msg.setMessage(Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                }

                msgList.add(msg);
            }
        }
        return msgList;
    }

    public void close() {
        try {
            connection.close();
            tableMsg.close();
            executorServiceMsg.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws Exception{
        HbaseNativeChatMessageService hbaseNativeChatMessageService = new HbaseNativeChatMessageService();
        System.out.println("**Get messages based on date, sender, receiver**");
        List<Msg> message = hbaseNativeChatMessageService.getMessage("2025-03-05", "17782621662", "13693589073");
        for(Msg msg:message){
            System.out.println(msg);
        }
        System.out.println("**Close HBase connection**");
        hbaseNativeChatMessageService.close();
    }
    
}
