import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

import static java.nio.file.StandardOpenOption.READ;

public class ReadingDocuments {
    public static  int nFileUsed = 0;

    public static void readDoc() throws IOException {
        File test2 = new File("C:\\Users\\onpep\\Desktop\\InformationRetrivial\\Project\\a.tsv");
        //File test2 = new File("C:\\Users\\edoar\\Documents\\Università\\Multim Inf Ret\\collectionReduction.tsv");
        ; //initializing a new ArrayList out of String[]'s
        int indexOfFile = 1;
        Preprocessing preproc = new Preprocessing();
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));

        LineIterator it = FileUtils.lineIterator(test2,"UTF-8");
        while (it.hasNext()) {
            //instantiate a new Inverted Index and Lexicon per block
            Lexicon lex = new Lexicon();
            InvertedIndex invInd = new InvertedIndex();
            nFileUsed++;
            int count = 0;

            System.out.println("ENTRO");
            while ( it.hasNext() && ( (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()) < (0.2*Runtime.getRuntime().maxMemory()) ) ) {
                String docCurrent = it.nextLine();
                String docText = new String (docCurrent.split(",")[1].getBytes(StandardCharsets.UTF_8));
                String docId = docCurrent.split(",")[0];
                ArrayList<Text> docPreprocessed = preproc.preprocess(docText,1);

                if(docPreprocessed!=null) {
                    for (Text term : docPreprocessed) {

                        lex.addElement(term, Long.parseLong(docId), invInd);
                    }
                }
                count ++;
                if ( count % 100000 == 0)
                    System.out.println(count);



            }
            System.out.println("BLOCCO CREATO");

            InvertedIndex.saveDocIDsOnFile("Inverted_Index_DocId_number_" + indexOfFile, lex);
            InvertedIndex.saveTFsOnFile("Inverted_Index_TF_number_" + indexOfFile, lex);
            lex.saveLexiconOnFile("Lexicon_number_"+indexOfFile);
            lex.clearLexicon();
            invInd.clearInvertedIndex();
            indexOfFile++;
        }
        now = LocalDateTime.now();
        System.out.println(dtf.format(now));
        System.out.println("INIZIO MERGING");
        if(nFileUsed!=1)
            Lexicon.mergeAllBlocks();
        now = LocalDateTime.now();
        System.out.println("Fine"+ dtf.format(now));
        //Compression
        long offsetFileLexicon = 0;
        long offsetFileInvertedDocId = 0;
        long offsetFileInvertedTf = 0;
        long offsetFileSkipInfo = 0;
        Path fileLex = Paths.get("Lexicon_number_1");
        FileChannel fcLex = FileChannel.open(fileLex, READ);
        for( int i = 0; i< fcLex.size();i= i+54) {

            ArrayList<Long> offsets = InvertedIndex.compression(offsetFileLexicon,"Lexicon_number_1",
                    "Inverted_Index_DocId_number_1",
                    "Inverted_Index_TF_number_1", offsetFileInvertedDocId,
                    offsetFileInvertedTf,offsetFileSkipInfo);

            offsetFileLexicon += 54;
            offsetFileSkipInfo = offsets.get(0);
            offsetFileInvertedDocId = offsets.get(1);
            offsetFileInvertedTf = offsets.get(2);
        }


    }



    public static void main(String argv[]) throws IOException {
        readDoc();


        //System.out.println( "\\u" + Integer.toHexString('÷' | 0x10000).substring(1) );
    }
}




