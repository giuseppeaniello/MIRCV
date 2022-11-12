import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ReadingDocuments {
    public static void readDoc() {
        File test2 = new File("C:\\Users\\onpep\\Desktop\\InformationRetrivial\\Project\\collection.tsv");
        ; //initializing a new ArrayList out of String[]'s
        Lexicon lex = new Lexicon();
        InvertedIndex invInd = new InvertedIndex();
        int indexFile = 0;
        try (BufferedReader TSVReader = new BufferedReader(new FileReader(test2))) {
            String line = null;

            long freeMemory = Runtime.getRuntime().freeMemory();
            long totalMemory = Runtime.getRuntime().totalMemory();

            while ((line = TSVReader.readLine()) != null) {
                if(freeMemory/totalMemory < 0.75) {
                    String docId = line.split("\\t")[0];
                    String doc = new String(line.split("\\t")[1].getBytes(), "UTF-8");
                    ArrayList<String> docPreproc = preprocessing(doc);
                    int i = -1;
                    for(String term : docPreproc){
                        i++;
                        lex.checkNewTerm(term, docId);
                        invInd.addTermToIndex(term, docId, i);
                    }
                }
                else{
                    lex.saveLexiconInDisk("Lexicon_number_" + indexFile);
                    invInd.saveIndexOnDisk("Inverted_Index_number_" + indexFile);
                    indexFile++;
                    lex.clear();
                    invInd.clear();
                }
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