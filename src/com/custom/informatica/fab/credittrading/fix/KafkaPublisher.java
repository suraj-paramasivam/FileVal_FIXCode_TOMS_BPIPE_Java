package com.custom.informatica.fab.credittrading.fix;
import com.custom.informatica.fab.credittrading.fix.bgcgfi;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;


public class KafkaPublisher {



    public static String getTimestamp(){
        return new Timestamp(System.currentTimeMillis()).toString();
    }

    public void publishRecord(String msgtype,int msgseqnum,String sendercompid,String sendingtime,String targetcompid,String mdreqid,String mdupdateaction,String fullsymbol,String typeofinstrument,String quotationmethod,String isin,String refpricequotedtype, int mdentrypx,String mdentrysize,String mdentrydate,String mdentrytime){
        Properties props =  new Properties();
        try {
            props.load(new FileReader("producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        KafkaProducer<String, bgcgfi> producer = new KafkaProducer<>(props);
        ArrayList<bgcgfi> bgcevents = new ArrayList<>();
        bgcevents.add(bgcgfi.newBuilder()
                .setMSGTYPE(msgtype)
                .setMSGSEQNUM(msgseqnum)
                .setSENDERCOMPID(sendercompid)
                .setSENDINGTIME(sendingtime)
                .setTARGETCOMPID(targetcompid)
                .setMDREQID(mdreqid)
                .setMDUPDATEACTION(mdupdateaction)
                .setFULLSYMBOL(fullsymbol)
                .setTYPEOFINSTRUMENT(typeofinstrument)
                .setQUOTATIONMETHOD(quotationmethod)
                .setISIN(isin)
                .setREFPRICEQUITEDTYPE(refpricequotedtype)
                .setMDENTRYPX(mdentrypx)
                .setMDENTRYSIZE(mdentrysize)
                .setMDENTRYDATE(mdentrydate)
                .setMDENTRYTIME(mdentrytime)
                .build());

        for(bgcgfi bgcevent:bgcevents) {
            ProducerRecord<String, bgcgfi> record = new ProducerRecord<>("credit_trading_bgc", bgcevent);
        }
    }

}
