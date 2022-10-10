import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ReadingDocuments {
    public static void readDoc() {
        File test2 = new File("C:\\Users\\onpep\\Desktop\\InformationRetrivial\\Project\\collection.tsv");
        ; //initializing a new ArrayList out of String[]'s
        try (BufferedReader TSVReader = new BufferedReader(new FileReader(test2))) {
            String line = null;
            while ((line = TSVReader.readLine()) != null) {
                int docid = Integer.parseInt(line.split("\\t")[0]);
                String doc = new String(line.split("\\t")[1].getBytes(),"UTF-8");
                System.out.println(doc);
            }
        } catch (Exception e) {
            System.out.println("Something went wrong");
        }
    }
    public static void main(String argv[]){
        readDoc();

            //System.out.println( "\\u" + Integer.toHexString('÷' | 0x10000).substring(1) );
    }
}