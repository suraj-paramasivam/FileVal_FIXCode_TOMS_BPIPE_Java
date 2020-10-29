package com.custom.informatica.fab.credittrading.bpipe;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.io.*;
import java.util.Calendar;

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Event.EventType;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.SessionOptions.ServerAddress;

public class bpipe_subscription
    {
        private static final String AUTH_SVC = "//blp/apiauth";

        private static final Name AUTHORIZATION_SUCCESS = Name.getName("AuthorizationSuccess");
        private static final Name TOKEN_SUCCESS = Name.getName("TokenGenerationSuccess");

        private static final Name EXCEPTIONS = Name.getName("exceptions");
        private static final Name FIELD_ID = Name.getName("fieldId");
        private static final Name REASON = Name.getName("reason");
        private static final Name ERROR_CODE = Name.getName("errorCode");
        private static final Name CATEGORY = Name.getName("category");
        private static final Name DESCRIPTION = Name.getName("description");
        private static final Name SlowConsumerWarning = Name.getName("SlowConsumerWarning");
        private static final Name SlowConsumerWarningCleared = Name.getName("SlowConsumerWarningCleared");

        private ArrayList<String> d_hosts;
        private int               d_port;
        private String            d_authOption;
        private String            d_name;
        private Identity          d_identity;
        private Session           d_session;
        private ArrayList<String> d_securities;
        private ArrayList<String> d_fields;
        private ArrayList<String> d_options;
        private SubscriptionList  d_subscriptions;
        private SimpleDateFormat  d_dateFormat;
        private String            d_service;
        private String jsonSubData=null;

        private boolean createSession()	throws IOException, InterruptedException
        {
            String authOptions = null;
            if(d_authOption.equalsIgnoreCase("APPLICATION")){
                // Set Application Authentication Option
                authOptions = "AuthenticationMode=APPLICATION_ONLY;";
                authOptions += "ApplicationAuthenticationType=APPNAME_AND_KEY;";
                // ApplicationName is the entry in EMRS.
                authOptions += "ApplicationName=" + d_name;
            } else {
                // Set User authentication option
                if (d_authOption.equalsIgnoreCase("NONE"))
                {
                    d_authOption = null;
                }
                else
                {
                    if (d_authOption.equalsIgnoreCase("USER_APP"))
                    {
                        // Set User and Application Authentication Option
                        authOptions = "AuthenticationMode=USER_AND_APPLICATION;";
                        authOptions += "AuthenticationType=OS_LOGON;";
                        authOptions += "ApplicationAuthenticationType=APPNAME_AND_KEY;";
                        // ApplicationName is the entry in EMRS.
                        authOptions += "ApplicationName=" + d_name;
                    }
                    else
                    {
                        if (d_authOption.equalsIgnoreCase("DIRSVC"))
                        {
                            // Authenticate user using active directory service property
                            authOptions = "AuthenticationType=DIRECTORY_SERVICE;";
                            authOptions += "DirSvcPropertyName=" + d_name;
                        }
                        else
                        {
                            // Authenticate user using windows/unix login name
                            authOptions = "AuthenticationType=OS_LOGON";
                        }
                    }
                }
            }

            System.out.println("authOptions = " + authOptions);
            SessionOptions sessionOptions = new SessionOptions();
            if (d_authOption != null)
            {
                sessionOptions.setAuthenticationOptions(authOptions);
            }

            ServerAddress[] servers = new ServerAddress[d_hosts.size()];
            for (int i = 0; i < d_hosts.size(); ++i) {
                servers[i] = new ServerAddress(d_hosts.get(i), d_port);
            }

            sessionOptions.setServerAddresses(servers);
            sessionOptions.setAutoRestartOnDisconnection(true);
            sessionOptions.setNumStartAttempts(d_hosts.size());
            sessionOptions.setDefaultSubscriptionService(d_service);

            System.out.print("Connecting to port " + d_port + " on server:");
            for (ServerAddress server: sessionOptions.getServerAddresses()) {
                System.out.print(" " + server);
            }
            System.out.println();
            d_session = new Session(sessionOptions, new SubscriptionEventHandler());

            return d_session.start();
        }

        private boolean authorize()
                throws IOException, InterruptedException {
            Event event;
            MessageIterator msgIter;

            EventQueue tokenEventQueue = new EventQueue();
            CorrelationID corrlationId = new CorrelationID(99);
            d_session.generateToken(corrlationId, tokenEventQueue);
            String token = null;
            int timeoutMilliSeonds = 10000;
            event = tokenEventQueue.nextEvent(timeoutMilliSeonds);
            if (event.eventType() == EventType.TOKEN_STATUS) {
                MessageIterator iter = event.messageIterator();
                while (iter.hasNext()) {
                    Message msg = iter.next();
                    System.out.println(msg.toString());
                    if (msg.messageType() == TOKEN_SUCCESS) {
                        token = msg.getElementAsString("token");
                    }
                }
            }
            if (token == null){
                System.err.println("Failed to get token");
                return false;
            }

            if (d_session.openService(AUTH_SVC)) {
                Service authService = d_session.getService(AUTH_SVC);
                Request authRequest = authService.createAuthorizationRequest();
                authRequest.set("token", token);

                EventQueue authEventQueue = new EventQueue();

                d_session.sendAuthorizationRequest(authRequest, d_identity,
                        authEventQueue, new CorrelationID(d_identity));

                while (true) {
                    event = authEventQueue.nextEvent();
                    if (event.eventType() == EventType.RESPONSE
                            || event.eventType() == EventType.PARTIAL_RESPONSE
                            || event.eventType() == EventType.REQUEST_STATUS) {
                        msgIter = event.messageIterator();
                        while (msgIter.hasNext()) {
                            Message msg = msgIter.next();
                            System.out.println(msg);
                            if (msg.messageType() == AUTHORIZATION_SUCCESS) {
                                return true;
                            } else {
                                System.err.println("Not authorized");
                                return false;
                            }
                        }
                    }
                }
            }
            return false;
        }

        /**
         * @param args
         */
        public static void main(String[] args) throws java.lang.Exception
        {
            System.out.println("Realtime Event Handler Example");
            bpipe_subscription example = new bpipe_subscription();
            example.run(args);
        }

        public bpipe_subscription()
        {
            d_hosts = new ArrayList<String>();
            d_port = 8194;
            d_authOption="";
            d_name="";
            d_session = null;

            d_service = "//blp/mktdata";
            d_securities = new ArrayList<String>();
            d_fields = new ArrayList<String>();
            d_options = new ArrayList<String>();
            d_subscriptions = new SubscriptionList();
            d_dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        }

        private void run(String[] args) throws Exception
        {
            if (!parseCommandLine(args)) return;
            if (!createSession()) {
                System.err.println("Failed to start session.");
                return;
            }

            if (d_authOption != null) {
                d_identity = d_session.createIdentity();
                if (!authorize()) {
                    return;
                }
            }

            if (d_authOption == null)
            {
                System.out.println("Subscribing...");
                d_session.subscribe(d_subscriptions);
            }
            else
            {
                System.out.println("Subscribing with Identity...");
                d_session.subscribe(d_subscriptions, d_identity);
            }

            // wait for enter key to exit application
            System.in.read();

            d_session.stop();
            System.out.println("Exiting.");
        }

        class SubscriptionEventHandler implements EventHandler
        {
            public void processEvent(Event event, Session session)
            {
                try {
                    switch (event.eventType().intValue())
                    {
                        case Event.EventType.Constants.SUBSCRIPTION_DATA:
                            processSubscriptionDataEvent(event, session);
                            break;
                        case Event.EventType.Constants.SUBSCRIPTION_STATUS:
                            processSubscriptionStatus(event, session);
                            break;
                        case Event.EventType.Constants.ADMIN:
                            MessageIterator msgIter = event.messageIterator();
                            while (msgIter.hasNext()) {
                                Message msg = msgIter.next();
                                if (msg.messageType() == SlowConsumerWarning) {
                                    System.out.println("Slow consumer warning!");
                                } else if (msg.messageType() == SlowConsumerWarningCleared) {
                                    System.out.println("Slow consumer warning cleared.");
                                } else {
                                    System.out.println(msg.toString());
                                }
                            }
                            break;
                        default:
                            processMiscEvents(event, session);
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private boolean processSubscriptionStatus(Event event, Session session)
                    throws Exception
            {

                System.out.println("Processing SUBSCRIPTION_STATUS: " + event.eventType().toString());
                MessageIterator msgIter = event.messageIterator();
                System.out.println(event);
                while (msgIter.hasNext()) {
                    Message msg = msgIter.next();
                    //System.out.println("MESSAGE: " + msg);
                    String topic = (String) msg.correlationID().object();
                    System.out.println(
                            d_dateFormat.format(Calendar.getInstance().getTime()) +
                                    ": " + topic + " - " + msg.messageType());

                    if (msg.hasElement(REASON)) {
                        // This can occur on SubscriptionFailure.
                        Element reason = msg.getElement(REASON);
                        if (reason.hasElement(ERROR_CODE, true))
                        {
                            // has error code
                            String category = "";
                            String description = "";
                            if (reason.hasElement(CATEGORY))
                            {
                                category = reason.getElement(CATEGORY).getValueAsString();
                            }
                            if (reason.hasElement(DESCRIPTION))
                            {
                                description = reason.getElement(DESCRIPTION).getValueAsString();
                            }

                            System.out.println("\t" +
                                    category + ": " + description);
                        }
                    }

                    if (msg.hasElement(EXCEPTIONS)) {
                        // This can occur on SubscriptionStarted if at least
                        // one field is good while the rest are bad.
                        Element exceptions = msg.getElement(EXCEPTIONS);
                        for (int i = 0; i < exceptions.numValues(); ++i) {
                            Element exInfo = exceptions.getValueAsElement(i);
                            Element fieldId = exInfo.getElement(FIELD_ID);
                            Element reason = exInfo.getElement(REASON);
                            System.out.println("\t" + fieldId.getValueAsString() +
                                    ": " + reason.getElement(CATEGORY).getValueAsString());
                        }
                    }
                    System.out.println("");
                }
                return true;
            }

            private boolean processSubscriptionDataEvent(Event event, Session session)
                    throws Exception
            {

                System.out.println("Processing SUBSCRIPTION_DATA");
                MessageIterator msgIter = event.messageIterator();
                while (msgIter.hasNext()) {
                    Message msg = msgIter.next();
                    String topic = (String) msg.correlationID().object();
                    System.out.println(
                            d_dateFormat.format(Calendar.getInstance().getTime()) +
                                    ": " + topic + " - " + msg.messageType());
                    jsonSubData="SubscriptionData = { " +
                            "topic: "+topic+
                             "messageType: "+msg.messageType();


                    int numFields = msg.asElement().numElements();

                    for (int i = 0; i < numFields; ++i) {
                        Element field = msg.asElement().getElement(i);
                        if (field.isNull()) {
                            System.out.println("\t\t" + field.name() + " is NULL");
                            continue;
                        }

                        processElement(field);
                    }
                }
                return true;
            }

            private void processElement(Element element) throws Exception
            {
                if (element.isArray())
                {
                    System.out.println("\t\t" + element.name());
                    // process array
                    int numOfValues = element.numValues();
                    for (int i = 0; i < numOfValues; ++i)
                    {
                        // process array data
                        processElement(element.getValueAsElement(i));
                    }
                }
                else if (element.numElements() > 0)
                {
                    System.out.println("\t\t" + element.name());
                    int numOfElements = element.numElements();
                    for (int i = 0; i < numOfElements; ++i)
                    {
                        // process child elements
                        processElement(element.getElement(i));
                    }
                }
                else
                {
                    // Assume all values are scalar.
                    System.out.println("\t\t" + element.name() + " = " +
                            element.getValueAsString());
                    jsonSubData.concat(element.name().toString()+":"+element.getValueAsString()+"}");
                }
            }

            private boolean processMiscEvents(Event event, Session session)
                    throws Exception
            {
                System.out.println("Processing " + event.eventType());
                MessageIterator msgIter = event.messageIterator();
                while (msgIter.hasNext()) {
                    Message msg = msgIter.next();
                    System.out.println(
                            d_dateFormat.format(Calendar.getInstance().getTime()) +
                                    ": " + msg.messageType() + "\n");
                }
                return true;
            }
        }

        private boolean readFile(ArrayList<String> list, String fileName)
        {
            boolean stateFlag = false;
            try {
                FileInputStream fstream = new FileInputStream(fileName);
                DataInputStream in = new DataInputStream(fstream);
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String data;
                // read security from file
                while ((data = reader.readLine()) != null) {
                    if (data.trim().length() > 0) {
                        // add security
                        list.add(data.trim());
                    }
                }
                fstream.close();
                stateFlag = true;
            } catch (Exception e){
                //Catch exception if any
                System.err.println("Error: " + e.getMessage());
            }

            return stateFlag;
        }

        private boolean parseCommandLine(String[] args)
        {
            String secFileName = "";
            String fldFileName = "";

            for (int i = 0; i < args.length; ++i) {
                if (args[i].equalsIgnoreCase("-s") && i + 1 < args.length) {
                    d_securities.add(args[++i]);
                }
                else if (args[i].equalsIgnoreCase("-f") && i + 1 < args.length) {
                    d_fields.add(args[++i]);
                }
                else if (args[i].equalsIgnoreCase("-Service") && i + 1 < args.length) {
                    d_service = args[++i];
                }
                else if (args[i].equalsIgnoreCase("-o") && i + 1 < args.length) {
                    d_options.add(args[++i]);
                }
                else if (args[i].equalsIgnoreCase("-fFile") && i + 1 < args.length) {
                    fldFileName = args[++i];
                }
                else if (args[i].equalsIgnoreCase("-sFile") && i + 1 < args.length) {
                    secFileName = args[++i];
                }
                else if (args[i].equalsIgnoreCase("-ip") && i + 1 < args.length) {
                    d_hosts.add(args[++i]);
                }
                else if (args[i].equalsIgnoreCase("-p") && i + 1 < args.length) {
                    d_port = Integer.parseInt(args[++i]);
                }
                else if(args[i].equalsIgnoreCase("-auth") && i + 1 < args.length) {
                    d_authOption = args[++i];
                }
                else if(args[i].equalsIgnoreCase("-n") && i + 1 < args.length) {
                    d_name = args[++i];
                }
                else if (args[i].equalsIgnoreCase("-h")) {
                    printUsage();
                    return false;
                }
            }

            // check for host ip
            if (d_hosts.size() == 0)
            {
                System.out.println("Missing host IP");
                printUsage();
                return false;
            }

            // check for application name
            if ((d_authOption.equalsIgnoreCase("APPLICATION")  || d_authOption.equalsIgnoreCase("USER_APP")) && (d_name.equalsIgnoreCase("")))
            {
                System.out.println("Application name cannot be NULL for application authorization.");
                printUsage();
                return false;
            }
            // check for Directory Service name
            if ((d_authOption.equalsIgnoreCase("DIRSVC")) && (d_name.equalsIgnoreCase("")))
            {
                System.out.println("Directory Service property name cannot be NULL for DIRSVC authorization.");
                printUsage();
                return false;
            }

            if (fldFileName.length() > 0) {
                if (!readFile(d_fields, fldFileName)) {
                    System.out.println("Unable to read field file: " + fldFileName);
                    return false;
                }
            }

            if (d_fields.size() == 0) {
                d_fields.add("LAST_PRICE");
            }

            if (secFileName.length() > 0) {
                if (!readFile(d_securities, secFileName)) {
                    System.out.println("Unable to read security file: " + secFileName);
                    return false;
                }
            }

            if (d_securities.size() == 0) {
                d_securities.add("IBM US Equity");
            }

            System.out.println("Subscription string(s):");
            for (String security : d_securities) {
                Subscription subscription = new Subscription(security, d_fields, d_options,
                        new CorrelationID(security));
                d_subscriptions.add(subscription);
                System.out.println(subscription.toString());
            }

            return true;
        }

        private void printUsage()
        {
            System.out.println("Usage:");
            System.out.println("    Retrieve realtime data ");
            System.out.println("        [-s         <security   = IBM US Equity>");
            System.out.println("        [-service   <service    = //blp/mktdata>");
            System.out.println("        [-f         <field      = LAST_PRICE>");
            System.out.println("        [-o         <subscriptionOptions>");
            System.out.println("        [-sFile 	<security list file>");
            System.out.println("        [-fFile 	<field list file>");
            System.out.println("        [-ip        <ipAddress	= localhost>");
            System.out.println("        [-p         <tcpPort	= 8194>");
            System.out.println("        [-auth      <authenticationOption = LOGON (default) or NONE or APPLICATION or DIRSVC or USER_APP>]");
            System.out.println("        [-n         <name = applicationName or directoryService>]");
            System.out.println("Notes:");
            System.out.println(" -Specify only LOGON to authorize 'user' using Windows login name.");
            System.out.println(" -Specify DIRSVC and name(Directory Service Property) to authorize user using directory Service.");
            System.out.println(" -Specify APPLICATION and name(Application Name) to authorize application.");
            System.out.println("Press ENTER to quit");
        }
    }


