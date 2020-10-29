package com.custom.informatica.fab.credittrading.datalicense;

import java.io.*;

public class ConvertISINFF2JSON {

    public static void main(String[] args) throws IOException {
        File file = new File("ISIN_TXT");
        PrintWriter pw = new PrintWriter("ISIN_LIST.json");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = null;
        String consJson = null;
        pw.write("{\"ISINLIST\": [");
        while ((line = br.readLine()) != null) {
            consJson = ("{\"@type\": \"Identifier\"," +
                    "\"identifierType\": \"ISIN\"," +
                    "\"identifierValue\":" + "\"" + line + "\"},");
            pw.write(consJson);
        }
        pw.write("]}");
        pw.flush();
        pw.close();
        br.close();

    }
}
