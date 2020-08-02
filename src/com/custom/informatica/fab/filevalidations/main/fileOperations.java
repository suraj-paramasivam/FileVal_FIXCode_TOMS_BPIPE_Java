/*
 * File - fileOperations.java
 * Author - Suraj Thyagarajan Paramasivam
 * Description - This program is intended to do file validations for FAB.
 * */
//Univosity Parser Imports - Need to add to Informatica Class path before execution

package com.custom.informatica.fab.filevalidations.main;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthParser;
import com.univocity.parsers.fixed.FixedWidthParserSettings;
import org.apache.commons.io.*;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.*;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

//Normal File Imports


public class fileOperations {
    public static HashMap<String, Integer> tfixedWidthMap;
    private final String pInputpfileLocation; //Input File Location variable/parameter
    private final List<String> pfileList; //File list variable/parameter
    private final List<String> pNotFoundList; //List containing the names of files not found in the given directory in comparison to the provided list
    private final String pDelimiter = ","; //Provided Delimiter - Change according to the object
    private final HashMap<String, Integer> olistofFieldsCount = new HashMap<String, Integer>();// Map to store number of fields in each file
    private final HashMap<String, Integer> olistofLineCount = new HashMap<String, Integer>();//Map to store line count of each file
    private final List<String> pFoundFileList; //list for the files found in the directory
    private final HashMap<String, String> olistofDelimiters = new HashMap<String, String>();//map to store delimiters for each file
    Logger logger = Logger.getLogger(fileOperations.class.getName());//Logger initiation
    String pfileLocation = "C:\\Suraj\\Personal\\Workspaces\\IntelliJ\\Infa_FAB\\testfiles\\"; // File location provided, to be used as parameter
    int pHeaderAvailable = 1;//parameter to denote availability of header in the csv file
    FileHandler logFileHandler;
    SimpleFormatter format = new SimpleFormatter();
    List<String[]> allRows;
    private int fileCount = -1; //Initialize count for number of lines in count
    private int notfoundFileCount = 0;//parameter for number of files not found in the directory
    private int tfileLevelFieldCount = 0;//file level field count, temporary variable
    private int tfileLevelLineCount = 0;//file level line count, temporary variable
    private HashMap<String, Integer> olistoffixedWidthFiles;


    //Constructor
    public fileOperations(String inputpfileLocation) {
        this.pInputpfileLocation = inputpfileLocation;
        this.pfileList = new LinkedList<String>();
        this.pNotFoundList = new LinkedList<String>();
        this.pFoundFileList = new LinkedList<String>();


    }

    public static void main(String[] args) throws IOException {

        List<String> pfileNameList = Arrays.asList("test.csv", "abc.csv", "xyz.csv");
        fileOperations fop = new fileOperations("C:\\Suraj\\Personal\\Workspaces\\IntelliJ\\Infa_FAB\\testfiles");
        fop.processList(pfileNameList);
        fop.logFileHandler = new FileHandler("filevalidations.log", true);
        fop.logger.addHandler(fop.logFileHandler);
        fop.logFileHandler.setFormatter(fop.format);


        System.out.println("number of files found " + fop.getFileCount());
        System.out.println("number of files not found " + fop.getNotfoundFileCount());
        if (fop.fileCountCheck(pfileNameList) == false) {
            System.out.print("Number of files don't match, missing files are " + fop.getpNotFoundList());
        }
        System.out.println("\nThe delimiter map is " + fop.mapFieldDelimiters(fop.pFoundFileList));
        System.out.println("Number of Fields" + fop.mapFieldCounts(fop.pFoundFileList));
        System.out.println("Map of Line Counts: " + fop.mapLineCounts(fop.pFoundFileList));
        fop.logFileHandler.close();
//        fop.validateRowLength(tfixedWidthMap, "C:\\Suraj\\Personal\\Workspaces\\IntelliJ\\Infa_FAB\\testfiles\\xyz.csv");
        List <String> temptest = new ArrayList<>();

        File[] f = fop.matchFilesByPattern("abc*");
        for (int i = 0; i < f.length; i++) {
            System.out.println("The Pattern file is " + f[i]);
            temptest.add(f[i].toString());
        }
    }

    public void processList(List<String> filenamelist) {
        try {
            for (String filename : filenamelist) {
                File f = new File(pInputpfileLocation, filename);
                if (f.exists()) {
                    logger.info("The file " + f + " exists");
                    fileCount += 1;
                    pFoundFileList.add(filename);
                } else {
                    logger.log(Level.WARNING, "The file " + f + " doesn't exist");
                    pNotFoundList.add(filename);
                    notfoundFileCount += 1;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public boolean fileCountCheck(List<String> filenamelist) {
        return fileCount == filenamelist.size();
    }

    public String findDelimiter(String filename) {
        CsvParserSettings csvParserSettings = new CsvParserSettings();
        csvParserSettings.detectFormatAutomatically();
        CsvParser parser = new CsvParser(csvParserSettings);
        List<String[]> rows = parser.parseAll(new File(pfileLocation + filename));
        CsvFormat csvformat = parser.getDetectedFormat();
        return csvformat.getDelimiterString();
    }

    public Map mapFieldDelimiters(List<String> filenamelist) {
        String filelevelDelimiter;
        for (String filename : filenamelist) {
            filelevelDelimiter = findDelimiter(filename);
            olistofDelimiters.put(filename, filelevelDelimiter);
        }
        return olistofDelimiters;
    }

    public Map mapFieldCounts(List<String> filenamelist) {
        for (String filename : filenamelist) {
            tfileLevelFieldCount = fieldCountCheck(filename);
            olistofFieldsCount.put(filename, tfileLevelFieldCount);
        }
        return olistofFieldsCount;
    }

    public int fieldCountCheck(String filename) {
        File f = new File(pfileLocation + filename);
        Scanner scanner;
        try {
            scanner = new Scanner(f);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return -1;
        }
        int fieldcount = 0;
        if (scanner.hasNextLine()) {
            fieldcount = scanner.nextLine().split(pDelimiter).length;
        }
        scanner.close();
        return fieldcount;
    }

    public int countLines(String filename) throws IOException {
        BufferedReader breader = new BufferedReader(new FileReader(pfileLocation + filename));
        String input;
        if (pHeaderAvailable == 1) {
            fileCount = -1;
        } else {
            fileCount = 0;
        }
        while ((input = breader.readLine()) != null) {
            fileCount++;
        }
        breader.close();
        return fileCount;
    }

    public Map mapLineCounts(List<String> filenamelist) throws IOException {
        for (String filename : filenamelist) {
            tfileLevelLineCount = countLines(filename);
            olistofLineCount.put(filename, tfileLevelLineCount);
        }
        return olistofLineCount;
    }

    public Map<String, Integer> getFixedwidthsfromFile(String filename) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String linefixedwidth = null;
        while ((linefixedwidth = br.readLine()) != null) {
            String[] parts = linefixedwidth.split(":");
            String fWidthFilefilename = parts[0].trim();
            Integer fWidths = Integer.parseInt(parts[1].trim());
            if (!fWidthFilefilename.equals("") && !fWidths.equals("")) {
                tfixedWidthMap.put(fWidthFilefilename, fWidths);
            }

        }
        return tfixedWidthMap;
    }

    public Map<String, Integer> validateRowLength(Map<String, Integer> fixedWidthMap, String filename) {
        FixedWidthFields fwf = new FixedWidthFields((LinkedHashMap<String, Integer>) fixedWidthMap);
        FixedWidthParserSettings settings = new FixedWidthParserSettings(fwf);
        settings.getFormat().setPadding('_');
        FixedWidthParser parser = new FixedWidthParser(settings);
        allRows = parser.parseAll(new File(pfileLocation + filename));
        int legthofrowsperfile = allRows.size();
        olistoffixedWidthFiles.put(filename, legthofrowsperfile);
        return olistoffixedWidthFiles;

    }


    public List<String> getpNotFoundList() {
        return pNotFoundList;
    }

    public int getFileCount() {
        return fileCount;
    }

    public int getNotfoundFileCount() {
        return notfoundFileCount;
    }

    public File[] matchFilesByPattern(String sample){
        File dir = new File(pfileLocation);
        FileFilter fileFilter = new WildcardFileFilter(sample);
        File[] files = dir.listFiles(fileFilter);
        return files;
    }




}
