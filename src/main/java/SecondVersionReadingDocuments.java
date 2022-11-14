import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.SortedTableMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class SecondVersionReadingDocuments {

   static int indexOfBlock;

    public SecondVersionReadingDocuments(){
        indexOfBlock = 0;
    }

    public static void readDoc(){
        File test2 = new File("C:\\Users\\edoar\\Documents\\Università\\Multim Inf Ret\\collectionReduction10.tsv");
        Dictionary dict = new Dictionary(indexOfBlock);
        OnFileInvertedIndex invInd = new OnFileInvertedIndex(indexOfBlock);
        try (BufferedReader TSVReader = new BufferedReader(new FileReader(test2))) {
            String line = null;
            long freeMemory;
            long totalMemory;
            while ((line = TSVReader.readLine()) != null) {
                freeMemory = Runtime.getRuntime().freeMemory();
                totalMemory = Runtime.getRuntime().totalMemory();
                if(freeMemory > totalMemory*0.8) {
                    String docId = line.split("\\t")[0];
                    String doc = new String(line.split("\\t")[1].getBytes(), "UTF-8");
                    ArrayList<String> docPreproc = preprocessing.preprocess(doc);
                    int i = -1;
                    for(String term : docPreproc){
                        i++;
                        dict.addTermToDictionary(term, docId);
                        invInd.addTermToIndex(term, docId, i);
                        // here we should do a sorting to have a correspondance between Lexicon and InvertedIndex
                    }
                }
                else{
                    indexOfBlock++;
                    dict.closeDictionary();
                    invInd.closeInvertedIndex();
                    dict = new Dictionary(indexOfBlock);
                    invInd = new OnFileInvertedIndex(indexOfBlock);
                }
                dict.closeDictionary();
                invInd.closeInvertedIndex();
            }
        } catch (Exception e) {
            System.out.println("Something went wrong");
        }
    }

    public static void main(String args[]){
        readDoc();

    }

}




