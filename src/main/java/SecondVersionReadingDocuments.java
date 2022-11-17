import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;


public class SecondVersionReadingDocuments {

   static int indexOfBlock;

    public SecondVersionReadingDocuments(){
        indexOfBlock = 0;
    }

    public static void readDoc2(){
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
                    ArrayList<String> docPreproc = preprocessing.preprocess(doc,1);
                    int i = -1;
                    for(String term : docPreproc){
                    //    System.out.println(term);   //to be eliminated
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
            }
            dict.closeDictionary();
            invInd.closeInvertedIndex();
        } catch (Exception e) {
            System.out.println("Something went wrong");
        }
    }

    public static void main(String args[]){
        readDoc2();

        /* words appeared for testing
            equal
            import
            success
            manhattan
            project
            scientif
            intellect
         */


        // test code for the Dictionary
        DB db = DBMaker.fileDB("dictionaryDB0.db").make();
        HTreeMap myMap = db.hashMap("dictionaryOnFile0").open();
        Integer a = (Integer) myMap.get("equal");
        System.out.println(a);
        Integer b = (Integer) myMap.get("import");
        System.out.println(b);
        Integer c = (Integer) myMap.get("success");
        System.out.println(c);
        Integer d = (Integer) myMap.get("manhattan");
        System.out.println(d);
        // dictionary funziona


        DB db2 = DBMaker.fileDB("invIndDB0.db").make();

        HTreeMap myMap2 = db2.hashMap("invertedIndexOnFile0").open();
        PostingList ps = (PostingList) myMap2.get("equal");
        for(Posting posting: ps.postingList) {
            System.out.print("DOC_ID:  " + posting.docID + "   ");
            System.out.print("POSITIONS: ");
            for(int position : posting.positions) {
                System.out.print(position + " ");
            }
        }
        System.out.println("");

        ps = (PostingList) myMap2.get("import");
        for(Posting posting: ps.postingList) {
            System.out.print("DOC_ID:  " + posting.docID + "   ");
            System.out.print("POSITIONS: ");
            for(int position : posting.positions) {
                System.out.print(position + " ");
            }
        }
        System.out.println("");

        ps = (PostingList) myMap2.get("success");
        for(Posting posting: ps.postingList) {
            System.out.print("DOC_ID:  " + posting.docID + "   ");
            System.out.print("POSITIONS: ");
            for(int position : posting.positions) {
                System.out.print(position + " ");
            }
        }
        System.out.println("");

        ps = (PostingList) myMap2.get("manhattan");
        for(Posting posting: ps.postingList) {
            System.out.print("DOC_ID:  " + posting.docID + "   ");
            System.out.print("POSITIONS: ");
            for(int position : posting.positions) {
                System.out.print(position + " ");
            }
        }
        System.out.println("");
    }

}




