package com.custom.informatica.fab.credittrading.fix;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import quickfix.*;
import quickfix.field.*;
import quickfix.fix50.MarketDataIncrementalRefresh;
import quickfix.fix50.MarketDataRequest;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;


public class FIXParser implements Application {
    public static final Logger logger = Logger.getLogger(com.custom.informatica.fab.credittrading.fix.FIXParser.class);
    public static String opValue = null;
    private static boolean initiatorStarted = false;
    private static volatile SessionID sessionID;
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private static int loginCounter = 0;

    public static SessionSettings settings;

    static {
        try {
            settings = new SessionSettings("res/initiator.cfg");
        } catch (ConfigError configError) {
            logger.error(configError);
        }
    }

    public static Application application;
    static {
        try {
            application = new FIXParser();
        } catch (ConfigError configError) {
            logger.error(configError);
        }
    }

    public static MessageStoreFactory messageStoreFactory = new FileStoreFactory(settings);
    public static LogFactory logFactory = new FileLogFactory(settings);
    public static MessageFactory messageFactory = new DefaultMessageFactory();
    public static Initiator initiator = null;



    public FIXParser() throws ConfigError {

    }


    public static void main(String[] args) throws Exception {
        PropertyConfigurator.configure("res/bgcgfi_logging.properties");
        if(loginCounter<2){
            userLogon();
        }
        else{
            logger.error("Issue with Login Credentials, please recheck and restart system");
            System.exit(1);
        }
        Session session=Session.lookupSession(sessionID);
        userRequest(session);
        Thread.sleep(5000);
        MDRequest(session);
       FIXParser fp = new FIXParser();

        shutdownLatch.await();

    }

    /**
     * UserLogon - only session level logon
     * @throws ConfigError
     */
    public static void userLogon() throws ConfigError {
        initiator = new SocketInitiator(application, messageStoreFactory, settings, logFactory,
                messageFactory);
        if (!initiatorStarted) {
            try {
                initiator.start();
                initiatorStarted = true;
                logger.debug("initator started set to true");
                while (sessionID == null) {
                    Thread.sleep(1000);
                }

            } catch (Exception e) {
                logger.info("Issue with login - Please turn on debug mode to see more details");
                logger.debug(e);
                loginCounter += 1;
                logger.error(e);
            }
        } else {

            for (SessionID sessionId : initiator.getSessions()) {
                Session.lookupSession(sessionId).logon();
                logger.info("User Logged On");
            }
            logger.debug("The session id is : " + sessionID);
            Session session = Session.lookupSession(sessionID);
            logger.debug("session created with session id : " + sessionID);

        }
        if(!initiator.isLoggedOn()){
            loginCounter+=1;
        }
    }

    /**
     * User Request to be sent for userlogon - sending 35=BE, picks username and password from cfg file
     * @param session
     * @throws Exception
     */

    public static void userRequest(Session session) throws Exception {
        logger.info("Starting User Request");

        Message userReq = messageFactory.create(sessionID.getBeginString(), MsgType.USER_REQUEST);
        userReq.setUtcTimeStamp(UserRequestID.FIELD, LocalDateTime.now());
        userReq.setInt(UserRequestType.FIELD, UserRequestType.LOG_ON_USER);
        userReq.setString(Username.FIELD, settings.getString("UserName"));
        logger.info("using username: " + settings.getString("UserName"));
        userReq.setString(Password.FIELD, settings.getString("Password"));

        try {

            if (loginCounter < 2) {
                session.sendToTarget(userReq, sessionID);

            } else {
                System.exit(1);
            }
        }
           catch(Exception e){
               logger.error("Error sending User Request: "+e);
               loginCounter+=1;
            }

    }

    /**
     * Market Data Request, as of now, only requesting incremental refresh. If Full refresh is required, change 265
     * @param session
     * @throws SessionNotFound
     * @throws InterruptedException
     */
        //Market Data  requests
        public static void MDRequest (Session session) throws SessionNotFound, InterruptedException {
            logger.info("Starting Market Data Request");
            UUID uuid = UUID.randomUUID();
            String randomUUIDString = uuid.toString();
            logger.debug("UUID created is: " + randomUUIDString);
            Message message = new Message();
            MarketDataRequest.NoMDEntryTypes group = new MarketDataRequest.NoMDEntryTypes();
            MarketDataRequest.NoRelatedSym sym = new MarketDataRequest.NoRelatedSym();
            MarketDataRequest marketDataRequest = new MarketDataRequest();

            // BeginString
            message.getHeader().setField(new StringField(8, "FIX.5.0"));

            //MsgType
            message.getHeader().setField(new StringField(35, "V"));

            // MDRequest ID
            message.setField(new StringField(262, randomUUIDString));
            //Subscription Request Type
            message.setField(new StringField(263, "1"));

            // Market Depth
            message.setField(new StringField(264, "0"));
            // MDUpdateType
            message.setField(new StringField(265, "1"));
            // Aggregated Book
            message.setField(new StringField(266, "N"));
            // NOMDEntryType
            message.setField(new StringField(267, "2"));
            // MDEntry Type
            group.setField(new StringField(269, "0"));
            message.addGroup(group);
            // MDEntry Type
            group.setField(new StringField(269, "1"));
            message.addGroup(group);
            // NORelatedSym
            message.setField(new StringField(146, "1"));
            sym.setField(new Symbol("ALL"));
            message.addGroup(sym);
            Session.sendToTarget(message, sessionID);
            logger.debug("Completed Sending Message, going to sleep for 2 seconds");
            Thread.sleep(2000);
        }

    /**
     * Decodes message using the message printer class. uses CTS_BoB_FIX44.xml from BGC. Needs to be changed if changes required by BGC
     * @param message
     * @throws ConfigError
     * @throws FieldNotFound
     */
        public void decodeMessage (Message message) throws ConfigError, FieldNotFound {
            logger.info("Entering Decode Message");
            //MessageFactory messageFactory = new MessageFactory();
            InputStream fix44Input = FIXParser.class.getResourceAsStream("CTS_BoB_FIX44.xml");
            logger.info("Using CTS_BoB_FIX44.xml as FIX Parser XML");
            DataDictionary dataDictionary = new DataDictionary(fix44Input);
            MessagePrinter mpr = new MessagePrinter();
            mpr.print(dataDictionary, message);


        }

    /**
     * Stop Function
     */

    public void stop () {
            shutdownLatch.countDown();
        }

    /**
     * Session Creation
     * @param sessionID
     */
        @Override
        public void onCreate (SessionID sessionID){

            logger.info("Session Created");

        }

    /**
     * onLogon Overridden Session
     * @param sessionID
     */
        @Override
        public void onLogon (SessionID sessionID){

            if(loginCounter>1){
                System.exit(1);
            }
            FIXParser.sessionID = sessionID;
            logger.info("Logon Completed");
            logger.debug("The session id for Logon is : " + sessionID);

        }

    /**
     * OnLogout Overridden Session
     * @param sessionID
     */

    @Override
        public void onLogout (SessionID sessionID){

            FIXParser.sessionID = null;
            logger.info("Logged out from BGC/GFI");
            logger.debug("Session id set to null");
            System.exit(1);
        }

        @Override
        public void toAdmin (Message message, SessionID sessionID){

            logger.info("ToAdmin");
            logger.debug("The message and session id are : " + message + sessionID);
            try {
                final String msgType = message.getHeader().getString(MsgType.FIELD);
                if (MsgType.LOGON.compareTo(msgType) == 0) {

                }
            } catch (FieldNotFound e) {
                e.printStackTrace();
                logger.error("Error sending to admin: " + e);
            }
        }

        @Override
        public void fromAdmin (Message message, SessionID sessionID) throws
        FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
            logger.debug("From Admin: "+ message);
        }

        @Override
        public void toApp (Message message, SessionID sessionID) throws DoNotSend {
            logger.debug("ToApp: " + message);
        }

        @Override
        public void fromApp (Message message, SessionID sessionID) throws
        FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        logger.debug("FromApp: " + message);




            try {
                decodeMessage(message);
            } catch (ConfigError configError) {
                logger.error(configError);
                configError.printStackTrace();
            }

        }

    }

    class MessagePrinter {
        public void print(DataDictionary dd, Message message) throws FieldNotFound, ConfigError {
            KafkaPublisher kp = new KafkaPublisher();
            String msgType = message.getHeader().getString(MsgType.FIELD);
            HashMap<String, String> headerFields = new HashMap<String, String>();
            HashMap<String, String> msgFields = new HashMap<String, String>();
            HashMap<String, String> trailerFields = new HashMap<String, String>();
            HashMap<String, String> allFields = new HashMap<String, String>();


           // headerFields = printFields("", dd, msgType, message.getHeader());
            System.out.println("message from printer: "+message);
            msgFields = printFields("", dd, msgType, message);
            //trailerFields = printFields("", dd, msgType, message.getTrailer());
           // allFields.putAll(headerFields);
            allFields.putAll(msgFields);
            //allFields.putAll(trailerFields);
            //System.out.println("headerfields: "+headerFields);
            System.out.println("messagefields: "+msgFields);
           // System.out.println("trailerfields: "+trailerFields);
            System.out.println("Map: " + allFields);
            kp.publishRecord(allFields);


        }

        public HashMap<String, String> printFields(String prefix, DataDictionary dd, String msgType, FieldMap fmp) throws ConfigError, FieldNotFound {
            Iterator fieldIterator = fmp.iterator();
            HashMap<String, String> fields = new HashMap<String, String>();

            while (fieldIterator.hasNext()) {
                Field field = (Field) fieldIterator.next();
                if (!isGroupCountField(dd, field)) {

                    String value = fmp.getString(field.getTag());
                    System.out.println(value);

                    if (dd.hasFieldValue(field.getTag())) {
                        value = dd.getValueName(field.getTag(), fmp.getString(field.getTag()) );
                        System.out.println("inside tag: "+field.getTag());
                        System.out.println("inside tag value: "+fmp.getString(field.getTag()));
                    }
                    fields.put(dd.getFieldName(field.getTag()), value);

                    System.out.println(prefix + dd.getFieldName(field.getTag()) + ":" + value);

                    Iterator groupsKeys = fmp.groupKeyIterator();
                    while (groupsKeys.hasNext()) {
                        int groupCountTag = (Integer) groupsKeys.next();
                        System.out.println(prefix + dd.getFieldName(groupCountTag) + ": count = "
                                + fmp.getInt(groupCountTag));
                        Group g = new Group(groupCountTag, 0);
                        int i = 1;
                        while (fmp.hasGroup(i, groupCountTag)) {
                            if (i > 1) {
                                System.out.println(prefix + "  ----");
                            }
                            fmp.getGroup(i, g);
                            printFields(prefix + "  ", dd, msgType, g);
                            i++;
                        }
                    }
                }

            }
            return fields;
        }

        private boolean isGroupCountField(DataDictionary dd, Field field) throws ConfigError {

            return dd.getFieldType(field.getTag()) == FieldType.NUMINGROUP;
        }
    }