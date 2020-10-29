package com.custom.informatica.fab.credittrading.toms;

import com.bloomberg.wag.sdk.RequestClient;
import com.bloomberg.wag.sdk.usermode.AuthenticationException;
import com.bloomberg.wag.sdk.usermode.DeviceModeFlow;
import com.custom.informatica.fab.credittrading.common.EncryptFilewithPass;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.security.GeneralSecurityException;
import java.util.*;


public class TomsTransaction {
    public static final Logger logger = Logger.getLogger(TomsTransaction.class);
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

    /**
     * main Function- Entry point
     *
     * @param args
     * @throws IOException
     * @throws AuthenticationException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, AuthenticationException, InterruptedException {
        //System.setProperty("https.proxyHost", "10.163.0.84");
        //System.setProperty("https.proxyPort", "8080");
        logger.info("using proxy host: 10.163.0.84 and proxy port; 8080");
        SCOPES.add("ts");
        String decryptFileName = args[0];
        String password = args[1];
        String pulltype = args[2];
        String endpoint=args[3];
        logger.debug("The arguments passed to this program are : " + decryptFileName + "******** - Password" +  pulltype+"endpoint: "+endpoint);


        //Call decrypt function
        EncryptFilewithPass df = new EncryptFilewithPass();
        try {
            EncryptFilewithPass.decryptFile(decryptFileName, password);
            logger.info("File decrypted");
        } catch (IOException | GeneralSecurityException e) {
            e.printStackTrace();
            logger.error("Something went wrong, please check stacktrace: " + e);
        }
        /*
         * Below call loads the credentials from the decrypted file and removes the decrypted file immediately.
         *
         */
        LoadCredentials lC = new LoadCredentials();
        logger.info("Credentials Loaded");
        CLIENT_ID = lC.loadClientID(decryptFileName);
        logger.debug("ClientID is " + CLIENT_ID);
        SECRET = lC.loadClientSecret(decryptFileName);

        System.out.println("Client ID: " + CLIENT_ID);

        File file = new File(decryptFileName);
        if (file.delete()) {
            logger.debug("Decrypted file deleted successfully");
            System.out.println("Decrypted file deleted successfully");
        } else {
            logger.error("Something went wrong with file delete, please delete this file manually - credentials/credential_toms.txt.decrypted.txt");
            System.out.println("Failed to delete Decrypted file, please delete this file manually - credentials/credential_toms.txt.decrypted.txt");
        }

        getAccessToken();
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
        //TH - Trade History
        //TS - Transaction Subscription
        //PR - Positions Request Response
        //PS - Positions Subscription
        //THD - Trade History Daily

        if (pulltype == "TH") {
            logger.info("you have selected Trade History, starting trade history pull ");
            HttpURLConnection response = client.getRequestBuilder("GET", "/ts/v1/trades/history")
                    .withQueryParam("px_num", px_num)
                    .withQueryParam("from_date", from_date)
                    .withQueryParam("to_date", to_date)
                    .doRequest();

            File fTH = new File(TradeHistoryFileName);
            FileOutputStream fosTH = new FileOutputStream(fTH);
            PrintWriter pwTH = new PrintWriter(fosTH);
            logger.info("writing data to file");

            try (Scanner scanner = new Scanner(response.getInputStream())) {
                while (scanner.hasNextLine()) {
                    pwTH.print(scanner.nextLine());
                }
                logger.info("write completed to file");
            } catch (Exception e) {

            }

            pwTH.flush();
            pwTH.close();
            logger.debug("printwriter flushed and closed, no security loopholes");

        }
        else  if (pulltype == "TS") {
            logger.info("you have selected Trade History, starting trade history pull with end point : " );
            HttpURLConnection response = client.getRequestBuilder("GET", "/ts/v1/trades/subscr")
                    .withQueryParam("px_num", px_num)
                    .withQueryParam("from_date", from_date)
                    .withQueryParam("to_date", to_date)

                    .doRequest();

            File fTH = new File(TradeHistoryFileName);
            FileOutputStream fosTH = new FileOutputStream(fTH);
            PrintWriter pwTH = new PrintWriter(fosTH);
            logger.info("writing data to file");

            try (Scanner scanner = new Scanner(response.getInputStream())) {
                while (scanner.hasNextLine()) {
                    pwTH.print(scanner.nextLine());
                }
                logger.info("write completed to file");
            } catch (Exception e) {

            }

            pwTH.flush();
            pwTH.close();
            logger.debug("printwriter flushed and closed, no security loopholes");
        }

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
                case "view":
                    refreshToken = Tokens[1];
                    break;
                case "from_date":
                    from_date = Tokens[1];
                    break;
                case "to_date":
                    to_date = Tokens[1];
                    break;

                case "TradeHistoryFileName":
                    TradeHistoryFileName = Tokens[1];
                    break;
                case "PositionsFileName":
                    PositionsFileName = Tokens[1];
                    break;
            }
            logger.debug("The parameters fetched from the properties file are " +
                    "px_num: " + px_num + "\n" + "view: " + view + "\n" + "from_date: " + from_date + "\n" +
                    "to_date: " + to_date + "\n" + "TradeHistoryFileName: " + TradeHistoryFileName + "\n" + "PositionsFileName: " + PositionsFileName);
        }

    }
}

class CustomFlow extends DeviceModeFlow {


    //private final String authCode;

    public CustomFlow(String host, int timeoutMillis, String clientId, String secret, List<String> scopes, String savedAccessToken, String savedRefreshToken, String savedIdToken) {
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



