import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

public class ReadingDocuments {


    public static void readDoc() {
        //File test2 = new File("C:\\Users\\onpep\\Desktop\\InformationRetrivial\\Project\\collection.tsv");
        File test2 = new File("C:\\Users\\edoar\\Documents\\Università\\Multim Inf Ret\\collectionReduction.tsv");
        ; //initializing a new ArrayList out of String[]'s
        int indexOfFile = 1;
        Lexicon lex = new Lexicon(indexOfFile);
        InvertedIndex invInd = new InvertedIndex(indexOfFile);
        try (BufferedReader TSVReader = new BufferedReader(new FileReader(test2))) {
            String line = null;
            long freeMemory;
            long totalMemory;
            while ((line = TSVReader.readLine()) != null) {
                //memoria occupata < 25% di memoria totale
                if( (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()) < 0.10*Runtime.getRuntime().totalMemory()  ){
                    // if(freeMemory - (totalMemory*0.25) > 0) {
                    freeMemory = Runtime.getRuntime().freeMemory();
                    String docId = line.split("\\t")[0];
                    String doc = new String(line.split("\\t")[1].getBytes(), "UTF-8");
                    ArrayList<Text> docPreproc = Preprocessing.preprocess(doc,1);
                    int i = -1;
                    for(Text term : docPreproc){
                        // System.out.print(term + "     ");
                        // System.out.println("memoria libera utilizzabile" + ((0.25*Runtime.getRuntime().totalMemory())-(Runtime.getRuntime().totalMemory() -Runtime.getRuntime().freeMemory() )));
                        i++;
                        lex.addElement(term, Long.parseLong(docId), invInd);
                        // here we should do a sorting to have a correspondance between Lexicon and InvertedIndex
                        // this isn't necessary because we are using TreeMap data structure in which values are ordered
                        // by alphabetical order (keys are Strings) in both Lexicon and InvertedIndex so the keys are the same
                        // and they are already ordered.
                    }
                }
                else{
                 //   System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                    lex.updateAllOffsetsTF(invInd);
                    invInd.saveDocIdCompressedOnFile(invInd.compressListOfDocIDsAndAssignOffsetsDocIDs(lex), "Inverted_Index_DocID_number_"+indexOfFile );
                    invInd.saveTFCompressedOnFile(invInd.compressListOfTFs(), "Inverted_Index_TF_number_"+indexOfFile);
                  //  lex.sortLexicon();
                    lex.saveLexiconOnFile("Lexicon_number_"+indexOfFile, indexOfFile);
                    indexOfFile++;
                   // System.out.println("BEFORE: " + Runtime.getRuntime().freeMemory());
                  //  lex.clearLexicon();
                   // invInd.clearInvertedIndex();
                    System.gc();
                   // System.out.println("AFTER: " + Runtime.getRuntime().freeMemory());

                    lex = new Lexicon(indexOfFile);
                    invInd = new InvertedIndex(indexOfFile);
                }
            }
            //ultimo file da creare
         //   System.out.println("ULTIMOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
            lex.updateAllOffsetsTF(invInd);
            invInd.saveDocIdCompressedOnFile(invInd.compressListOfDocIDsAndAssignOffsetsDocIDs(lex), "Inverted_Index_DocID_number_"+indexOfFile );
            invInd.saveTFCompressedOnFile(invInd.compressListOfTFs(), "Inverted_Index_TF_number_"+indexOfFile);
            lex.sortLexicon();
            lex.saveLexiconOnFile("Lexicon_number_"+indexOfFile, indexOfFile);
            indexOfFile++;
            lex.clearLexicon();
            invInd.clearInvertedIndex();
        } catch (Exception e) {
            System.out.println("Something went wrong " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String argv[]){
        readDoc();

        //System.out.println( "\\u" + Integer.toHexString('÷' | 0x10000).substring(1) );
    }
}




