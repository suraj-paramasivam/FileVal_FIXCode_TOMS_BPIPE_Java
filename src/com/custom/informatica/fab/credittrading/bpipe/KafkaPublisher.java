package com.custom.informatica.fab.credittrading.bpipe;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Kafka Publisher class
 */
public class KafkaPublisher {

    public static final Logger loggerk = Logger.getLogger(KafkaPublisher.class);
    public static String topic_kafka;

    public static String getTimestamp() {
        return new Timestamp(System.currentTimeMillis()).toString();
    }

    /**
     * Publish Records
     * @param parameters
     */

    public void publishRecord(HashMap<String, String> parameters) {
        PropertyConfigurator.configure("res/bgcgfi_logging.properties");
        Properties props = new Properties();
        try {
            props.load(new FileReader("src/com/custom/informatica/fab/credittrading/fix/kafka.properties"));
            loggerk.info("Kafka: Picked properties file from kafka.properties");
            props.put("acks", "all");
            topic_kafka = props.getProperty("topic");
            loggerk.debug("Kafka: Bootstrap Server List - " + props.getProperty("bootstrap.servers"));
            if (loggerk.isDebugEnabled()) {
                AdminClient ac = AdminClient.create(props);
                Set<String> Topics = ac.listTopics().names().get();
                for (String t : Topics) {
                    loggerk.debug("Kafka: Topic - " + t);
                }
            }

        } catch (IOException e) {
            loggerk.error("Kafka: " + e);

        } catch (InterruptedException e) {
            loggerk.error("Kafka: " + e);

        } catch (ExecutionException e) {
            loggerk.error("Kafka: " + e);

        }

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        loggerk.debug("Kafka: kafka producer object created");
//        ArrayList<bgcgfi> bgcevents = new ArrayList<>();
        String msgtype = null;
        String msgseqnum = null;
        String sendercompid = null;
        String targetcompid = null;
        String sendingtime = null;
        String mdreqid = null;
        String mdupdateaction = null;
        String fullsymbol = null;
        String isin = null;
        String mdentrypx = null;
        String mdentrysize = null;
        String mdentrydate = null;
        String mdentrytime = null;
        String mdentrytype = null;
        String mdentryid = null;
        String tradingsesionid = null;
        String orderid = null;
        String fullMsg = null;


        for (Map.Entry mapElement : parameters.entrySet()) {
            if (mapElement.getKey().equals("MsgType")) {
                msgtype = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("MsgSeqNum")) {
                msgseqnum = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("SenderCompID")) {
                sendercompid = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("SendingTime")) {
                sendingtime = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("TargetCompID")) {
                targetcompid = ((String) mapElement.getValue());
            }

            //MsgFields


            if (mapElement.getKey().equals("Symbol")) {
                fullsymbol = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("SecurityID")) {
                isin = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("MDReqID")) {
                mdreqid = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("MDUpdateAction")) {
                mdupdateaction = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("MDEntryType")) {
                mdentrytype = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("MDEntryID")) {
                mdentryid = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("MDEntryPx")) {
                mdentrypx = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("MDEntrySize")) {
                mdentrysize = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("MDEntryDate")) {
                mdentrydate = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("MDEntryTime")) {
                mdentrytime = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("TradingSessionID")) {
                tradingsesionid = ((String) mapElement.getValue());
            }
            if (mapElement.getKey().equals("OrderID")) {
                orderid = ((String) mapElement.getValue());
            }
            fullMsg = "MsgType:" + msgtype + "|" + "MsgSeqNum:" + msgseqnum + "|" + "SenderCompID:" + sendercompid + "|" + "SendingTime:" + sendingtime + "|" +
                    "TargetCompID:" + targetcompid + "|" + "MDReqID:" + mdreqid + "|" + "MDUpdateAction:" + mdupdateaction + "|" + "FullSymbol:" + fullsymbol + "|" +
                    "ISIN:" + isin + "|" + "MDEntryPx:" + mdentrypx + "|" + "MDEntrySize:" + mdentrysize + "|" + "MDEntryType:" + mdentrytype + "|" + "MDEntryDate:" + mdentrydate + "|" + "MDEntryTime:" + mdentrytime;
            loggerk.debug("Kafka: Full message - " + fullMsg);
//            bgcevents.add(bgcgfi.newBuilder()
//
//                    .setMSGTYPE(msgtype)
//                    .setMSGSEQNUM(msgseqnum)
//                    .setSENDERCOMPID(sendercompid)
//                    .setSENDINGTIME(sendingtime)
//                    .setTARGETCOMPID(targetcompid)
//                    .setMDREQID(mdreqid)
//                    .setMDUPDATEACTION(mdupdateaction)
//                    .setFULLSYMBOL(fullsymbol)
//                    .setISIN(isin)
//                    .setMDENTRYPX(mdentrypx)
//                    .setMDENTRYSIZE(mdentrysize)
//                    .setMDENTRYDATE(mdentrydate)
//                    .setMDENTRYTIME(mdentrytime)
//                    .build());
        }
//        for (bgcgfi bgc : bgcevents) {

        ProducerRecord<Integer, String> record = new ProducerRecord<>(topic_kafka, Integer.parseInt(msgseqnum), fullMsg);
        loggerk.debug("Producer record created");
        try {
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        loggerk.info("kafka: No Exceptions, All is Well");
                    } else {
                        loggerk.error("Kafka: Something went wrong in sending to Kafka, I am the call back function");
                        System.exit(1);
                    }
                }
            });
            producer.flush();
            producer.close();
            loggerk.debug("Published message to Kafka: " + msgseqnum + " Full Message: " + fullMsg);
        } catch (Exception e) {
            loggerk.error("Kafka: " + e);

        }
    }

}

