import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ReadingDocuments {
    public static  int nFileUsed = 0;

    public static void readDoc() throws IOException {
        //File test2 = new File("C:\\Users\\onpep\\Desktop\\InformationRetrivial\\Project\\collectionReduction.tsv");
        File test2 = new File("C:\\Users\\edoar\\Documents\\Università\\Multim Inf Ret\\collection.tsv");
        ; //initializing a new ArrayList out of String[]'s
        int indexOfFile = 1;

        LineIterator it = FileUtils.lineIterator(test2,"UTF-8");
        while (it.hasNext()) {
            //instantiate a new Inverted Index and Lexicon per block
            Lexicon lex = new Lexicon();
            InvertedIndex invInd = new InvertedIndex();
            nFileUsed++;

            while ( it.hasNext() && ( (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()) < (0.2*Runtime.getRuntime().maxMemory()) ) ) {
                String docCurrent = it.nextLine();
                String docText = new String (docCurrent.split("\\t")[1].getBytes(StandardCharsets.UTF_8));
                String docId = docCurrent.split("\\t")[0];
                ArrayList<Text> docPreprocessed = Preprocessing.preprocess(docText,0);
                for(Text term : docPreprocessed){
                    lex.addElement(term, Long.parseLong(docId), invInd);
                }
                if (Long.parseLong(docId) % 1000 == 0)
                    System.out.println(docId);
            }
            System.out.println("BLOCCO CREATO");

            InvertedIndex.saveDocIDsOnFile("DocIDsNumber " + indexOfFile, lex);
            InvertedIndex.saveTFsOnFile("TFsNumber " + indexOfFile, lex);
            lex.saveLexiconOnFile("Lexicon_number_"+indexOfFile);
            lex.clearLexicon();
            invInd.clearInvertedIndex();
            indexOfFile++;
        }
        //if(nFileUsed!=1)
            //Lexicon.mergeAllBlocks();

    }

    public static void testAddingElementsToDictionary() throws IOException {
        //File test2 = new File("C:\\Users\\onpep\\Desktop\\InformationRetrivial\\Project\\collectionReduction.tsv");
        File test2 = new File("C:\\Users\\edoar\\Documents\\Università\\Multim Inf Ret\\collection.tsv");
        ; //initializing a new ArrayList out of String[]'s
        LineIterator it = FileUtils.lineIterator(test2,"UTF-8");
        while (it.hasNext()) {
            //instantiate a new Inverted Index and Lexicon per block
            Lexicon lex = new Lexicon();
            InvertedIndex invInd = new InvertedIndex();
            nFileUsed++;

            while ( it.hasNext() && ( (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()) < (0.2*Runtime.getRuntime().maxMemory()) ) ) {
                String docCurrent = it.nextLine();
                String docText = new String (docCurrent.split("\\t")[1].getBytes(StandardCharsets.UTF_8));
                String docId = docCurrent.split("\\t")[0];
                ArrayList<Text> docPreprocessed = Preprocessing.preprocess(docText,0);
                for(Text term : docPreprocessed){
                    lex.addElement(term, Long.parseLong(docId), invInd);
                }
                if (Long.parseLong(docId) % 1000 == 0)
                    System.out.println(docId);
            }
            System.out.println("BLOCCO CREATO, SIAMO ARRIVATI AL DOC N° " + it.nextLine().split("\\t")[0]);
        }
    }

    public static void main(String argv[]) throws IOException {
        readDoc();


        //System.out.println( "\\u" + Integer.toHexString('÷' | 0x10000).substring(1) );
    }
}




