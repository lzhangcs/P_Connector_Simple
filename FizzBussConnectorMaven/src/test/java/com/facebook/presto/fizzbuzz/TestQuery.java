package com.facebook.presto.fizzbuzz;

import java.util.HashMap;
import java.util.Map;
import java.io.FileWriter;
import java.io.IOException;
import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestQuery {

    public static void writeMap2CsvFile(HashMap<Integer, String> map, String filepath)
    {
        final String[] header = new String[] { "id", "group"};
        String eol = System.getProperty("line.separator");

        try (FileWriter writer = new FileWriter(filepath, false))
        {
            writer.append(header[0]).append(',').append(header[1]).append(eol);
            for (Map.Entry<Integer, String> entry : map.entrySet())
            {
                writer.append(Integer.toString(entry.getKey())).append(',').append(entry.getValue()).append(eol);
            }
        }
        catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }


    public static String extractChoice(String s)
    {
        String choice = null;
        Pattern p = Pattern.compile("\"([^\"]*)\"");
        Matcher m = p.matcher(s);
        if (m.find()) {
            choice = m.group(1);
        }
        return choice;
    }


    public static void main(String[] args) throws IOException {

        // read in user's sql command from stdin, for example: select * from table for "Fizz"
        System.out.println("Please type in your SQL statement:\n");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String s = br.readLine();

        String choice = extractChoice(s);

        FizzBuzzConnector fbConnector = new FizzBuzzConnector();

        HashMap<Integer, String> outmap = fbConnector.genhashmap(choice);

        // Write Query result to .csv file
        String csvfile = "C:\\temp\\" + choice + ".csv";
        writeMap2CsvFile(outmap, csvfile);

       //Print out query result to console
        outmap.forEach((key, value) -> System.out.println(key + " is " + value));

        System.out.println("Done!\n");

    }

}