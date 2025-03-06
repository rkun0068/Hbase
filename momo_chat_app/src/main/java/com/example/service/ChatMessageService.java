package com.example.service;
// momo chat service
import java.util.List;
import com.example.entity.Msg;
public interface ChatMessageService {
    List<Msg> getMessage(String date,String sender,String receiver) throws Exception;
    void close() ;
}
