/*import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

public class ReadingDocuments {
    public static void readDoc() {
        //File test2 = new File("C:\\Users\\onpep\\Desktop\\InformationRetrivial\\Project\\collection.tsv");
        File test2 = new File("C:\\Users\\edoar\\Documents\\Università\\Multim Inf Ret\\collectionReduction.tsv");
        ; //initializing a new ArrayList out of String[]'s
        Lexicon lex = new Lexicon();
        InvertedIndex invInd = new InvertedIndex();
        int indexFile = 0;
        try (BufferedReader TSVReader = new BufferedReader(new FileReader(test2))) {
            String line = null;
            long freeMemory;
            long totalMemory;
            while ((line = TSVReader.readLine()) != null) {
                freeMemory = Runtime.getRuntime().freeMemory();
                totalMemory = Runtime.getRuntime().totalMemory();
                if(freeMemory > totalMemory*0.25) {
                    String docId = line.split("\\t")[0];
                    String doc = new String(line.split("\\t")[1].getBytes(), "UTF-8");
                    ArrayList<String> docPreproc = preprocessing.preprocess(doc,1);
                    int i = -1;
                    for(String term : docPreproc){
                        i++;
                        lex.addTermToLexicon(term, docId);
                        invInd.addTermToIndex(term, docId, i);
                        // here we should do a sorting to have a correspondance between Lexicon and InvertedIndex
                        // this isn't necessary because we are using TreeMap data structure in which values are ordered
                        // by alphabetical order (keys are Strings) in both Lexicon and InvertedIndex so the keys are the same
                        // and they are already ordered.
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
            //ultimo file da creare
            lex.saveLexiconInDisk("Lexicon_number_" + indexFile);
            invInd.saveIndexOnDisk("Inverted_Index_number_" + indexFile);
            lex.clear();
            invInd.clear();
        } catch (Exception e) {
            System.out.println("Something went wrong");
        }
    }
    public static void main(String argv[]){
        readDoc();

            //System.out.println( "\\u" + Integer.toHexString('÷' | 0x10000).substring(1) );
    }
}

*/


