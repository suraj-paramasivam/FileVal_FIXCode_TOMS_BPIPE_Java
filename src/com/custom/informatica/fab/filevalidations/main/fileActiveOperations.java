package com.custom.informatica.fab.filevalidations.main;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class fileActiveOperations {
    private final String vInputFileLocation;
    private final List<String> pfileList;
    String headerline;
    private int pHeaderAvailable = 1;

    public fileActiveOperations(String filelocation) {
        this.vInputFileLocation = filelocation;
        this.pfileList = new LinkedList<String>();

    }

    public static void main(String[] args) {
        fileActiveOperations faop = new fileActiveOperations("C:\\Suraj\\Personal\\Workspaces\\IntelliJ\\Infa_FAB\\testfiles\\");
        List<String> fileListActiveOperations = Arrays.asList("test.csv", "abc.csv", "xyz.csv");
        try {
            faop.execFileOperations(fileListActiveOperations);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execFileOperations(List<String> filenamelist) throws IOException {
        for (String filename : filenamelist) {
            File f = new File(vInputFileLocation, filename);
            if (f.exists()) {
                removeBlankLines(filename.toString());
                removeDuplicates(filename.toString());
            } else {
                continue;
            }
        }
    }

    public void removeBlankLines(String filename) {
        try {
            File inputFile = new File(vInputFileLocation + filename);
            Scanner file = new Scanner(inputFile);
            File temp_file = new File(vInputFileLocation + "temp_" + filename);
            PrintWriter pw = new PrintWriter(temp_file);


            while (file.hasNext()) {
                String line = file.nextLine();
                if (!line.isEmpty()) {
                    pw.write(line);
                    pw.write("\n");
                }
            }

            file.close();
            pw.close();
            try {
                inputFile.delete();
                temp_file.renameTo(inputFile);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            Logger.getLogger(fileActiveOperations.class.getName()).log(Level.SEVERE, null, e);
        }
    }

    public void removeDuplicates(String filename) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(vInputFileLocation + filename));
        Set<String> lines = new HashSet<String>();
        String line;
        if (pHeaderAvailable == 1) {
            headerline = reader.readLine();
        }

        while ((line = reader.readLine()) != null) {
            lines.add(line);
        }
        reader.close();
        BufferedWriter writer = new BufferedWriter(new FileWriter(vInputFileLocation + filename));
        if (pHeaderAvailable == 1) {
            writer.write(headerline);
            writer.newLine();
        }
        for (String uniqueLine : lines) {
            writer.write(uniqueLine);
            writer.newLine();
        }
        writer.close();
    }



}


