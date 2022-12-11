import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ReadingDocuments {


    public static void readDoc() throws IOException {
        File test2 = new File("C:\\Users\\onpep\\Desktop\\InformationRetrivial\\Project\\collectionReduction.tsv");
        //File test2 = new File("C:\\Users\\edoar\\Documents\\Università\\Multim Inf Ret\\collectionReduction.tsv");
        ; //initializing a new ArrayList out of String[]'s
        int indexOfFile = 1;

        LineIterator it = FileUtils.lineIterator(test2,"UTF-8");

        while (it.hasNext()) {
            //instantiate a new Inverted Index and Lexicon per block
            Lexicon lex = new Lexicon(indexOfFile);
            InvertedIndex invInd = new InvertedIndex(indexOfFile);

            while ( it.hasNext() && ( (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()) < (0.02*Runtime.getRuntime().maxMemory()) ) ) {
                String docCurrent = it.nextLine();
                String docText = new String (docCurrent.split("\\t")[1].getBytes(StandardCharsets.UTF_8));
                String docId = docCurrent.split("\\t")[0];
                ArrayList<Text> docPreprocessed = Preprocessing.preprocess(docText,1);
                for(Text term : docPreprocessed){
                    lex.addElement(term, Long.parseLong(docId), invInd);
                }
                //System.out.println(docId);
                if(Long.parseLong(docId) % 1000 == 0)
                    System.out.println(docId);
            }
            System.out.println("BLOCCO COSRUITO");
            lex.updateAllOffsetsTF(invInd);
            invInd.saveTForDocIDsCompressedOnFile(invInd.compressListOfDocIDsAndAssignOffsetsDocIDs(lex), "Inverted_Index_DocID_number_"+indexOfFile, 0 );
            invInd.saveTForDocIDsCompressedOnFile(invInd.compressListOfTFs(), "Inverted_Index_TF_number_"+indexOfFile, 0);
            lex.sortLexicon();
            lex.saveLexiconOnFile("Lexicon_number_"+indexOfFile, indexOfFile);
            lex.clearLexicon();
            invInd.clearInvertedIndex();
            indexOfFile++;
        }
 /*       try (BufferedReader TSVReader = new BufferedReader(new FileReader(test2))) {
            String line = null;

            while ((line = TSVReader.readLine()) != null) {
                //memoria occupata < 25% di memoria totale


                String docId = line.split("\\t")[0];
                String doc = new String(line.split("\\t")[1].getBytes(), "UTF-8");
                ArrayList<Text> docPreproc = Preprocessing.preprocess(doc,1);

                for(Text term : docPreproc){
                    long freeMemory = Runtime.getRuntime().freeMemory();
                    long totalMemory = Runtime.getRuntime().totalMemory();
                        // System.out.print(term + "     ");
                        // System.out.println("memoria libera utilizzabile" + ((0.25*Runtime.getRuntime().totalMemory())-(Runtime.getRuntime().totalMemory() -Runtime.getRuntime().freeMemory() )));
                    if( totalMemory-freeMemory< (0.75*totalMemory) ){
                        lex.addElement(term, Long.parseLong(docId), invInd);
                    }
                    else{
                        lex.updateAllOffsetsTF(invInd);
                        invInd.saveDocIdCompressedOnFile(invInd.compressListOfDocIDsAndAssignOffsetsDocIDs(lex), "Inverted_Index_DocID_number_"+indexOfFile );
                        invInd.saveTFCompressedOnFile(invInd.compressListOfTFs(), "Inverted_Index_TF_number_"+indexOfFile);
                            //  lex.sortLexicon();
                        lex.saveLexiconOnFile("Lexicon_number_"+indexOfFile, indexOfFile);
                            //  lex.clearLexicon();
                            //  invInd.clearInvertedIndex();
                        indexOfFile++;
                            // System.out.println("AFTER: " + Runtime.getRuntime().freeMemory());

                    }
                        // here we should do a sorting to have a correspondance between Lexicon and InvertedIndex
                        // this isn't necessary because we are using TreeMap data structure in which values are ordered
                        // by alphabetical order (keys are Strings) in both Lexicon and InvertedIndex so the keys are the same
                        // and they are already ordered.
                }


            }
            //ultimo file da creare
            lex.updateAllOffsetsTF(invInd);
            invInd.saveDocIdCompressedOnFile(invInd.compressListOfDocIDsAndAssignOffsetsDocIDs(lex), "Inverted_Index_DocID_number_"+indexOfFile );
            invInd.saveTFCompressedOnFile(invInd.compressListOfTFs(), "Inverted_Index_TF_number_"+indexOfFile);
            //lex.sortLexicon();
            lex.saveLexiconOnFile("Lexicon_number_"+indexOfFile, indexOfFile);
            indexOfFile++;
           // lex.clearLexicon();
            //invInd.clearInvertedIndex();
        } catch (Exception e) {
            System.out.println("Something went wrong " + e.getMessage());
            e.printStackTrace();
        }*/
    }

    public static void main(String argv[]) throws IOException {
        readDoc();

        //System.out.println( "\\u" + Integer.toHexString('÷' | 0x10000).substring(1) );
    }
}




