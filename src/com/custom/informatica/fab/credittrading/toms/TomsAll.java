package com.custom.informatica.fab.credittrading.toms;

import com.bloomberg.wag.sdk.RequestClient;
import com.bloomberg.wag.sdk.usermode.AuthenticationException;
import com.bloomberg.wag.sdk.usermode.DeviceModeFlow;
import com.custom.informatica.fab.credittrading.common.EncryptFilewithPass;
import okhttp3.HttpUrl;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.net.HttpURLConnection;
import java.security.GeneralSecurityException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Primary Class
 * List of Static Variables
 * logger, SCOPES, accessToken, refreshToken, idToken, hostname, px_num, view, from_date, to_date, TradeHistoryFileName, PositionsFileName, generatedToken, CLIENT_ID, SECRET
 */
public class TomsAll {
    public static final Logger logger = Logger.getLogger(TomsAll.class);
    private static final List<String> SCOPES = new ArrayList<String>();
    public static String accessToken = null;
    public static String refreshToken = null;
    public static String idToken = null;
    public static String hostname = null;
    public static String px_num = null;
    public static String view = null;
    public static String from_date = null;
    public static String to_date = null;
    public static String TradeHistoryFileName = null;
    public static String PositionsFileName = null;
    public static Map<String, String> generatedToken = null;
    private static String CLIENT_ID = null;
    private static String SECRET = null;
    private static String endpoint = null;
    private static String pulltype=null;
    public static String traderbooks=null;

    /**
     * Argument 1 - decrypt filename
     * Argument 2 - Password for decrypting file
     * Argument 3 - Pulltype - TH for Trade History, PR for Position Request
     * Argument 4 - endpoint - Required for Positions request, for Transactions, this is dummy value
     * Argument 5 - hostname - https://beta.api.bloomberg.com or https://api.bloomberg.com
     * Argument 6 - From Date - This is required for Transactions, for positions, this is a dummy value
     * Argument 7 - To Date - This is required for Transactions, for positions, it is a dummy value
     * Using Scope TS
     *
     * @param args
     * @throws IOException
     * @throws AuthenticationException
     * @throws InterruptedException
     * @throws ParseException
     */
    public static void main(String[] args) throws IOException, AuthenticationException, InterruptedException, ParseException {
        //System.setProperty("https.proxyHost", "10.163.0.84");
        //System.setProperty("https.proxyPort", "8080");
        PropertyConfigurator.configure("res/toms_logging.properties");
        logger.info("using proxy host: 10.163.0.84 and proxy port; 8080");
        SCOPES.add("ts");
        String decryptFileName = args[0];
        String password = args[1];
        pulltype = args[2];
        endpoint=args[3];
        hostname=args[4];
        from_date=args[5];
        to_date=args[6];
        logger.debug("The arguments passed to this program are : \n" + decryptFileName + "******** - Password\n" +  pulltype+"endpoint: "+endpoint+"\n hostname: "+hostname+
                "\nfrom_date: "+from_date+"\n to_date: "+to_date);


        //Call decrypt function

        try {
            EncryptFilewithPass.decryptFile(decryptFileName, password);
            logger.info("File decrypted");
        } catch (IOException | GeneralSecurityException e) {
            e.printStackTrace();
            logger.error("Something went wrong with decryption of secret file, please check stacktrace: " + e);
            System.exit(1);
        }
        /*
         * Below call loads the credentials from the decrypted file and removes the decrypted file immediately.
         *
         */
        LoadCredentials lC = new LoadCredentials();
        logger.info("Credentials Loaded");
        String decryptedFileName=decryptFileName+".decrypted.txt";
        CLIENT_ID = lC.loadClientID(decryptedFileName);
        logger.debug("ClientID is " + CLIENT_ID);
        SECRET = lC.loadClientSecret(decryptedFileName);
        logger.debug("Secret is : "+SECRET);

        File file = new File(decryptedFileName);
        if (file.delete()) {
            logger.debug("Decrypted file deleted successfully");

        } else {
            logger.error("Something went wrong with file delete, please delete this file manually - credentials/credential_toms.txt.decrypted.txt");

        }
        getParameters();
        getAccessToken();
        logger.debug("Access Token get completed");
        final CustomFlow flow = new CustomFlow(
                hostname,
                60 * 1000,
                CLIENT_ID,
                SECRET,
                SCOPES,
                accessToken,
                refreshToken,
                idToken

        );
        logger.info("Using flow parametes  hostname:"+hostname+" client id:"+CLIENT_ID+" access token: "+accessToken+" refresh token: "+refreshToken+" id token: "+idToken);
        UUID uuid = UUID.randomUUID();
        String randomUUIDString = uuid.toString();

        //  flow.refreshTokens();
        if (!(accessToken.isEmpty())) {

            File f = new File("OAT_TOMS");
            logger.debug("Token File picked up");
            if (System.currentTimeMillis() - f.lastModified() > 3600000) {
                logger.info("Time since last refresh is more than 1 hour, sending refresh request to BBG");
                flow.refreshTokens();
                generatedToken = flow.getTokens();
                writeToFile(generatedToken);
            }
        } else {
            final String loginUrl = flow.authenticate();
            logger.info("you should not be seeing this message, if you are seeing, the SOP for authentication was not followed, please follow SOP and reauthenticate");
            logger.info("You will need to run this java program manually, if SOP was not follwed and provide the url on console to someone who can authenticate. After authentication is completed, please press enter");
            System.out.println(loginUrl);
            logger.info(loginUrl);
            System.out.println("Hit enter after log-in...");
            //noinspection ResultOfMethodCallIgnored
            //System.in.read();
            //Thread.sleep(90000);


            // Fetch required tokens after logged in
            flow.fetchTokens();
            generatedToken = flow.getTokens();
            writeToFile(generatedToken);
        }
        RequestClient client = flow.getRequestClient();
        logger.debug("request client object created");

        Date today = new Date();


        long millis = System.currentTimeMillis();
        Date now = new Date(millis);
        //TH - Trade History - Current Release Sep 2020
        //TS - Transaction Subscription
        //PR - Positions Request Response - Current Release - Sep 2020
        //PS - Positions Subscription
        //THD - Trade History Daily


//Trade History
        if (pulltype.equals("TH")) {
            logger.info("you have selected Trade History, starting trade history pull ");



            HttpURLConnection response = client.getRequestBuilder("GET", "/ts/v1/trades/history")
                    .withQueryParam("px_num", px_num)
                    .withQueryParam("view","TRHFABView")
                    .withQueryParam("from_date", from_date)
                    .withQueryParam("to_date", to_date)
                    .withQueryParam("trader",traderbooks)
                    .doRequest();


            File fTH = new File(TradeHistoryFileName);
            if(!fTH.exists()){
                fTH.createNewFile();
            }
            FileWriter fwTH = new FileWriter(fTH,false);
            BufferedWriter bwTH = new BufferedWriter(fwTH);
            PrintWriter pwTH = new PrintWriter(bwTH);
            logger.info("writing data to file");

            try (Scanner scanner = new Scanner(response.getInputStream())) {
                while (scanner.hasNextLine()) {
                    pwTH.print(scanner.nextLine());
                }
                logger.info("write completed to file");
            } catch (Exception e) {
                logger.debug("Trade history"+e);
            }
            pwTH.flush();
            pwTH.close();
            fwTH.close();

            logger.debug("printwriter flushed and closed, no security loopholes");
            logger.info("Program Finished");
        }

        //Positions Request
        else  if (pulltype.equals("PR")) {
            logger.info("you have selected Positions Request Response, starting positions history pull with end point : " +endpoint);
            logger.debug("Using parameters for Positions query: "+"px_num: "+px_num+"View name: PositionFABView");
            HttpURLConnection response = client.getRequestBuilder("GET", "/ts/v1/positions/Trader/"+endpoint)
                    .withQueryParam("px_num", px_num)
                    .withQueryParam("PositionFABView")
                    .doRequest();

            File fPR = new File(PositionsFileName+"_"+endpoint);

            FileWriter fwPR = new FileWriter(fPR,false);
            BufferedWriter bwPR = new BufferedWriter(fwPR);
            PrintWriter pwPR = new PrintWriter(bwPR);
            logger.info("writing data to file");

            try (Scanner scanner = new Scanner(response.getInputStream())) {
                while (scanner.hasNextLine()) {
                    pwPR.print(scanner.nextLine());
                }
                logger.info("write completed to file");
            } catch (Exception e) {
                logger.debug("Positions Req/Response for : "+endpoint +e);
            }
            pwPR.flush();
            pwPR.close();
            fwPR.close();

            logger.debug("printwriter flushed and closed, no security loopholes");

            logger.info("Program Finished");
        }

        else{logger.info("Wrong Argument passed for Pulltype");
        System.exit(1);}


    }


    public static Date subtractDays(Date date, int days) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.add(Calendar.DATE, -days);

        return cal.getTime();
    }

    public static void writeToFile(Map<String, String> map) throws IOException {

        File f = new File("OAT_TOMS");
        FileOutputStream fos = new FileOutputStream(f);
        PrintWriter pw = new PrintWriter(fos);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            pw.println(entry.getKey() + "=" + entry.getValue());
        }
        pw.flush();
        pw.close();
        fos.close();
    }

    private static void print(HttpURLConnection response) throws IOException {
        System.out.println(response.getRequestMethod() + " " + response.getURL());

        System.out.println("Response:");

        System.out.println(response.getHeaderFields());

        try (Scanner scanner = new Scanner(response.getInputStream())) {
            while (scanner.hasNextLine()) {
                System.out.println(scanner.nextLine());
            }
        } catch (Exception e) {
            Scanner scanner = new Scanner(response.getErrorStream());
            while (scanner.hasNextLine()) {
                System.out.println(scanner.nextLine());
            }
        }

        System.out.println();
    }

    /**
     * This function gets the access token from the file where it is stored.
     *
     * @throws IOException
     */
    public static void getAccessToken() throws IOException {

        File f = new File("OAT_TOMS");
        FileReader reader = new FileReader(f);
        BufferedReader br = new BufferedReader(reader);
        StringBuffer buffer = new StringBuffer();
        String line;

        while ((line = br.readLine()) != null) {
            String[] Tokens = line.split("=", 2);
            switch (Tokens[0]) {
                case "access_token":
                    accessToken = Tokens[1];
                    break;
                case "refresh_token":
                    refreshToken = Tokens[1];
                    break;
                case "id_token":
                    idToken = Tokens[1];
                    break;
            }
        }

    }

    /**
     * This function gets the parameters required for this program from Toms.properties. All variables are static variables
     *
     * @throws IOException
     */
    public static void getParameters() throws IOException {

        File fp = new File("Toms.properties");
        FileReader reader = new FileReader(fp);
        BufferedReader br = new BufferedReader(reader);
        StringBuffer buffer = new StringBuffer();
        String line;

        while ((line = br.readLine()) != null) {
            String[] Tokens = line.split("=", 2);
            switch (Tokens[0]) {
                case "px_num":
                    px_num = Tokens[1];
                    break;
                case "traderbooks":
                    traderbooks = Tokens[1];
                    break;
                case "TradeHistoryFileName":
                    TradeHistoryFileName = Tokens[1];
                    break;
                case "PositionsFileName":
                    PositionsFileName = Tokens[1];
                    break;
            }
            logger.debug("The parameters fetched from the properties file are " +
                    "px_num: " + px_num + "\n" +  "TradeHistoryFileName: " + TradeHistoryFileName + "\n" + "PositionsFileName: " + PositionsFileName);
        }

    }
}

class CustomFlow_P extends DeviceModeFlow {


    //private final String authCode;

    public CustomFlow_P(String host, int timeoutMillis, String clientId, String secret, List<String> scopes, String savedAccessToken, String savedRefreshToken, String savedIdToken) {
        super(host, timeoutMillis, clientId, secret, scopes);
        this.accessToken = savedAccessToken;
        this.refreshToken = savedRefreshToken;
        this.idToken = savedIdToken;
        //this.authCode = "11daeeb20e7dfff14ca917f3";

    }

//    public void customfetchTokens() throws IOException, AuthenticationException {
//
//    }
}



