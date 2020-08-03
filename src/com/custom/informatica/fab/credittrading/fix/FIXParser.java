package com.custom.informatica.fab.credittrading.fix;

import quickfix.*;
import quickfix.field.MsgType;
import quickfix.field.Symbol;
import quickfix.fix50.MarketDataRequest;

import java.io.Console;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.*;

import static quickfix.MessageUtils.parse;
import quickfix.DataDictionary;


public class FIXParser implements Application {

    private static boolean initiatorStarted = false;
    private static volatile SessionID sessionID;
    private final Initiator initiator = null;

    public static void main(String[] args) throws ConfigError, FileNotFoundException, InterruptedException, SessionNotFound {
        SessionSettings settings = new SessionSettings("res/initiator.config");
                Application application = new FIXParser();
        MessageStoreFactory messageStoreFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new ScreenLogFactory(true, true, true);
        MessageFactory messageFactory = new DefaultMessageFactory();
        if (!initiatorStarted) {
            Initiator initiator = new SocketInitiator(application, messageStoreFactory, settings, logFactory, messageFactory);
            initiator.start();
            initiatorStarted = true;
            while (sessionID == null) {
                Thread.sleep(1000);
            }

            System.out.println("the session id is" + sessionID);
            Session session = Session.lookupSession(sessionID);
            userLogon(session);
            //productRequest(session);
             MDRequest(session);





        }
    }


    public static void userLogon(Session session) throws SessionNotFound, InterruptedException {
        Message message = new Message();
        // BeginString
        message.getHeader().setField(new StringField(8, "FIX.4.4"));

        //MsgType
        message.getHeader().setField(new StringField(35, "BE"));

        // SenderCompID
        message.getHeader().setField(new StringField(49, "MD02_FAB_FIXSTG5"));

        // TargetCompID, with enumeration
        message.getHeader().setField(new StringField(56, "FIXCTSBOBGW"));

        //Username
        message.getHeader().setField(new StringField(553, "bbnd_fab_md2"));

        // Password
        message.getHeader().setField(new StringField(554, "bgctest123"));

        // UserRequestID
        message.getHeader().setField(new StringField(923, "661516808"));

        // UserRequestType
        message.getHeader().setField(new StringField(924, "1"));

        System.out.println(message);
        Session.sendToTarget(message, sessionID);
        Thread.sleep(5000);

    }

    //Product requests
    public static void productRequest(Session session) throws SessionNotFound, InterruptedException {
        Message message = new Message();
        // BeginString
        message.getHeader().setField(new StringField(8, "FIX.5.0"));

        //MsgType
        message.getHeader().setField(new StringField(35, "x"));

        // SenderCompID
        message.getHeader().setField(new StringField(49, "MD02_FAB_FIXSTG5"));

        // TargetCompID, with enumeration
        message.getHeader().setField(new StringField(56, "FIXCTSBOBGW"));
        // SecurityListRequestType
        message.getHeader().setField(new StringField(559, "4"));

        //SecurityReqID
        message.getHeader().setField(new StringField(320, "661516809"));


        // SecurityListRequestType
        message.getHeader().setField(new StringField(263, "1"));

        System.out.println(message);
        Session.sendToTarget(message, sessionID);
        Thread.sleep(5000);
    }

    //Market Data  requests
    public static void MDRequest(Session session) throws SessionNotFound, InterruptedException {

        UUID uuid = UUID.randomUUID();
        String randomUUIDString = uuid.toString();
        Message message = new Message();
        MarketDataRequest.NoMDEntryTypes group = new MarketDataRequest.NoMDEntryTypes();
        MarketDataRequest.NoRelatedSym sym = new MarketDataRequest.NoRelatedSym();
        MarketDataRequest marketDataRequest = new MarketDataRequest();

        // BeginString
        message.getHeader().setField(new StringField(8, "FIX.5.0"));

        //MsgType
        message.getHeader().setField(new StringField(35, "V"));

        // SenderCompID
        message.getHeader().setField(new StringField(49, "MD02_FAB_FIXSTG5"));

        // TargetCompID, with enumeration
        message.getHeader().setField(new StringField(56, "FIXCTSBOBGW"));
        // TargetCompID, with enumeration
        message.setField(new StringField(262, randomUUIDString));
        // TargetCompID, with enumeration
        message.setField(new StringField(263, "1"));

        // TargetCompID, with enumeration
        message.setField(new StringField(264, "0"));

        // TargetCompID, with enumeration
        message.setField(new StringField(266, "N"));
        // Subscription Request Type
        message.setField(new StringField(267, "2"));
        // Subscription Request Type
        group.setField(new StringField(269, "0"));
        message.addGroup(group);
        // Subscription Request Type
        group.setField(new StringField(269, "1"));
        message.addGroup(group);


        // Subscription Request Type
        message.setField(new StringField(146, "1"));
        sym.setField(new Symbol("ALL"));

        // Subscription Request Type

        message.addGroup(sym);

        System.out.println(message);
        Session.sendToTarget(message, sessionID);

        Thread.sleep(5000);
        Scanner scanner = new Scanner(System.in);
        //Initial Read
        while(scanner.hasNextLine()){
            List<String> msgs = new ArrayList<String>();
            Scanner lineScanner = new Scanner(scanner.nextLine());
            while(lineScanner.hasNext()){
                msgs.add(lineScanner.next());
            }
            lineScanner.close();
            System.out.println("The Received Messages are : "+msgs);
        }
        scanner.close();


    }

    public void decodeMessage() throws ConfigError {
        //MessageFactory messageFactory = new MessageFactory();
        DefaultMessageFactory messageFactory = new DefaultMessageFactory();
        InputStream fix44Input = FIXParser.class.getResourceAsStream("/FIX44_tt.xml");
        DataDictionary dataDictionary = new DataDictionary(fix44Input);

    }

    @Override
    public void onCreate(SessionID sessionID) {
        System.out.println("OnCreate");
    }

    @Override
    public void onLogon(SessionID sessionID) {
        System.out.println("OnLogon");
        FIXParser.sessionID = sessionID;


    }

    @Override
    public void onLogout(SessionID sessionID) {
        System.out.println("OnLogout");
        FIXParser.sessionID = null;
    }

    @Override
    public void toAdmin(Message message, SessionID sessionID) {
        System.out.println("ToAdmin");
        try {
            final String msgType = message.getHeader().getString(MsgType.FIELD);
            if (MsgType.LOGON.compareTo(msgType) == 0) {

            }
        } catch (FieldNotFound e) {
            e.printStackTrace();
        }
    }

    @Override
    public void fromAdmin(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, RejectLogon {
        System.out.println("FromAdmin");
    }

    @Override
    public void toApp(Message message, SessionID sessionID) throws DoNotSend {
        System.out.println("ToApp: " + message);
    }

    @Override
    public void fromApp(Message message, SessionID sessionID) throws FieldNotFound, IncorrectDataFormat, IncorrectTagValue, UnsupportedMessageType {
        System.out.println("FromApp");
    }



}

class MessagePrinter{
    public void print(DataDictionary dd, Message message) throws FieldNotFound {
        String msgType = message.getHeader().getString(MsgType.FIELD);

    }

    public void printFields(String prefix, DataDictionary dd, String msgType,FieldMap fmp) throws ConfigError, FieldNotFound {
        Iterator fieldIterator = fmp.iterator();
        while (fieldIterator.hasNext()) {
            Field field = (Field) fieldIterator.next();
            if (!isGroupCountField(dd, field)) {
                String value = fmp.getString(field.getTag());
                if (dd.hasFieldValue(field.getTag())) {
                    value = dd.getValueName(field.getTag(), fmp.getString(field.getTag()) + "(" + value + ")");

                }
                System.out.println(prefix + dd.getFieldName(field.getTag()) + ":" + value);

                Iterator groupsKeys = fmp.groupKeyIterator();
                while (groupsKeys.hasNext()) {
                    int groupCountTag = ((Integer) groupsKeys.next()).intValue();
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
    }
    private boolean isGroupCountField(DataDictionary dd, Field field) throws ConfigError {

        return dd.getFieldType(field.getTag()) == FieldType.NUMINGROUP;
    }
}