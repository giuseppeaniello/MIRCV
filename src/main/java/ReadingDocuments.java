import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import static java.nio.file.StandardOpenOption.READ;

public class ReadingDocuments {

    public static  int nFileUsed = 0;

    public static void readDoc(int flag) throws IOException {

        //Read from file the dataset with all DocIds and
        File test2 = new File("collextion.tsv");

        DocumentTable documentTab = new DocumentTable();
        Preprocessing preproc = new Preprocessing();
        //Set of time to visualize the current time
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

        System.out.println("Starting Spimi-Inverted: "+dtf.format(LocalDateTime.now()));
        LineIterator it = FileUtils.lineIterator(test2,"UTF-8");

        while (it.hasNext()) {

            //instantiate a new Inverted Index and Lexicon per block
            Lexicon lex = new Lexicon();
            InvertedIndex invInd = new InvertedIndex();
            //For each block numberFileUsed is increased
            nFileUsed++;
            //Check to keep control of the RAM usage, when is over a threshold, the process is stopped and a new block
            // is created
            while ( it.hasNext() && ( (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory()) <
                    (0.1*Runtime.getRuntime().maxMemory()) ) ) {

                String docCurrent = it.nextLine();
                String docText = new String (docCurrent.split("\\t")[1].getBytes(StandardCharsets.UTF_8));
                String docId = docCurrent.split("\\t")[0];
                //Preprocessing of document before adding it in the lexicon
                ArrayList<Text> docPreprocessed = preproc.preprocess(docText);
                //Check if it is null(document without words)
                if(docPreprocessed!=null) {
                    for (Text term : docPreprocessed) {
                        //Each term is added in the lexicon and in the InvertedIndex
                        lex.addElement(term, Long.parseLong(docId), invInd);
                    }
                }
                // aggiunge il docID e la sua lunghezza alla document table
                DocumentTable.getDocTab().put(Long.parseLong(docId), docPreprocessed.size());
            }
            //Open all file and instanced the corresponding file channel
            RandomAccessFile lexFile = new RandomAccessFile(new File("Lexicon_number_"+nFileUsed), "rw");
            FileChannel lexChannel = lexFile.getChannel();
            RandomAccessFile invertedDocIdFile = new RandomAccessFile(new File("Inverted_Index_DocId_number_" + nFileUsed), "rw");
            FileChannel invDocIdsChannel = invertedDocIdFile.getChannel();
            RandomAccessFile invDocTfsFile = new RandomAccessFile(new File("Inverted_Index_TF_number_" + nFileUsed), "rw");
            FileChannel invertedTfsChannel = invDocTfsFile.getChannel();


            //Saving block of Lexicon, InvertedIndex with Tfs and InvertedIndex with DocIds
            InvertedIndex.saveDocIDsOnFile( lex, invDocIdsChannel);
            InvertedIndex.saveTFsOnFile( lex,invertedTfsChannel);
            lex.saveLexiconOnFile(lexChannel);
            //Close FileChannel used
            lexChannel.close();
            invDocIdsChannel.close();
            invertedTfsChannel.close();
            //Remove all variable used
            lex.clearLexicon();
            invInd.clearInvertedIndex();

        }
        //At end of the creation of the block, document table is saved and in the last position is saved the avarage length
        documentTab.calculateAverageLength();
        //Differentiation that includes stopwords and stemming or not
        if(flag == 1)
            documentTab.saveDocumentTable("document_table_stemmed_and_stopword_removed");
        else
            documentTab.saveDocumentTable("document_table_without_stemming_and_stopword_removal");
        System.out.println("End creation blocks "+ dtf.format(LocalDateTime.now()) );
        System.out.println("Number blocks created: "+nFileUsed);
        System.out.println("Start merge phase "+ dtf.format(LocalDateTime.now()));
        if(nFileUsed!=1)
            Lexicon.mergeAllBlocks();
        System.out.println("End merge phase "+ dtf.format(LocalDateTime.now()));

        //Compression
        long offsetFileLexicon = 0;
        long offsetFileInvertedDocId = 0;
        long offsetFileInvertedTf = 0;
        long offsetFileSkipInfo = 0;
        long offsetLexSkip = 0;

        RandomAccessFile lexFile = new RandomAccessFile(new File("Lexicon_Merge_number_"+(nFileUsed-1)), "r");

        FileChannel lexChannel = lexFile.getChannel();
        RandomAccessFile invDocIdFile = new RandomAccessFile(new File("Inverted_Index_Merge_DocId_number_"+(nFileUsed-1)), "r");

        FileChannel invDocIdChannel = invDocIdFile.getChannel();
        RandomAccessFile invTFFile = new RandomAccessFile(new File("Inverted_Index_Merge_TF_number_"+(nFileUsed-1)), "r");


        FileChannel invTFChannel = invTFFile.getChannel();

        ///
        RandomAccessFile lexFileAfterCompression = new RandomAccessFile(new File("Lexicon"), "rw");
        FileChannel lexChannelAfterCompression = lexFileAfterCompression.getChannel();
        String pathInvDocId;
        if (flag == 1){

            pathInvDocId = "InvertedDocIdStemmedAndStopwordRemoved";
        }
        else{
            pathInvDocId = "InvertedDocIdWithoutStemmingAndStopwordRemoving";
        }
        RandomAccessFile invDocAfterCompression = new RandomAccessFile(new File(pathInvDocId), "rw");
        FileChannel invDocIdChannelAfterCompression = invDocAfterCompression.getChannel();
        String pathInvTf;
        if(flag == 1){
            pathInvTf ="InvertedTFStemmedAndStopwordRemoved";
        }
        else{
            pathInvTf = "InvertedTFWithoutStemmingAndStopwordRemoving";
        }
        RandomAccessFile invTFFileAfterCompression = new RandomAccessFile(new File(pathInvTf), "rw");
        FileChannel invTFChannelAfterCompression = invTFFileAfterCompression.getChannel();
        String pathSkipInfo;
        if(flag == 1){
            pathSkipInfo = "SkipInfoStemmedAndStopwordRemoved";
        }
        else{
            pathSkipInfo = "SkipInfoWithoutStemmingAndStopwordRemoving";
        }
        RandomAccessFile skipInfoFile = new RandomAccessFile(new File(pathSkipInfo), "rw");
        FileChannel skipInfoChannel = skipInfoFile.getChannel();

        System.out.println("Start compression phase "+ dtf.format(LocalDateTime.now()));
        System.out.println("InvertedDocId size before compression: "+ invDocIdFile.length());
        System.out.println("InvertedTF size before compression: "+ invTFFile.length());
        for( int i = 0; i< lexChannel.size();i= i+54) {
/////////
            ArrayList<Long> offsets = InvertedIndex.compression(i, lexChannel, invDocIdChannel, invTFChannel,
                    offsetFileInvertedDocId, offsetFileInvertedTf, offsetFileSkipInfo, offsetLexSkip,lexChannelAfterCompression,
                    invDocIdChannelAfterCompression, invTFChannelAfterCompression,skipInfoChannel);

            offsetFileSkipInfo = offsets.get(0);
            offsetFileInvertedDocId = offsets.get(1);
            offsetFileInvertedTf = offsets.get(2);
            offsetLexSkip = offsets.get(3);
        }
        lexChannel.close();
        invDocIdChannel.close();
        invTFChannel.close();
        Lexicon.deleteFile("Lexicon_Merge_number_"+(nFileUsed-1));
        Lexicon.deleteFile("Inverted_Index_Merge_DocId_number_"+(nFileUsed-1));
        Lexicon.deleteFile("Inverted_Index_Merge_TF_number_"+(nFileUsed-1));
        System.out.println("InvertedDocId size after compression: "+ invDocAfterCompression.length());
        System.out.println("InvertedTF size after compression: "+ invTFFileAfterCompression.length());
        System.out.println("End compression phase "+ dtf.format(LocalDateTime.now()));

        System.out.println("Start building final Lexicon with TermUpperBound "+dtf.format(LocalDateTime.now()));
        Lexicon lex = Lexicon.readAllLexicon(lexChannelAfterCompression);
        LexiconFinal lexFinal = new LexiconFinal();
        for(Text term : lex.lexicon.keySet()){

            LexiconValueFinal lexValueFinal = new LexiconValueFinal();
            lexValueFinal.setCf(lex.lexicon.get(term).getCf());
            lexValueFinal.setDf(lex.lexicon.get(term).getDf());
            lexValueFinal.setnBlock(lex.lexicon.get(term).getnBlock());
            lexValueFinal.setOffsetSkipBlocks(lex.lexicon.get(term).getOffsetSkipBlocks());
            //Compute termUpperBoundTFIDF
            Ranking rankTFIDF = lex.computeScoresForATermTfidfForUpperBound(term,skipInfoChannel,invDocIdChannelAfterCompression,invTFChannelAfterCompression);
            lexValueFinal.setTermUpperBoundTFIDF(rankTFIDF.computeTermUpperBound());
            //Compute termUpperBoundBM25
            Ranking rankBM25 = lex.computeScoresForATermBM25ForUpperBound(term, documentTab,skipInfoChannel,invDocIdChannelAfterCompression,invTFChannelAfterCompression);
            lexValueFinal.setTermUpperBoundBM25(rankBM25.computeTermUpperBound());
            lexFinal.lexicon.put(term,lexValueFinal);

        }
        String lexPath;
        if(flag == 1) {
            lexPath = "LexiconFinalStemmedAndStopWordRemoved";
        }
        else {
            lexPath = "LexiconFinalWithoutStemmingAndStopWordRemoval";
        }
        RandomAccessFile finalLexicon = new RandomAccessFile(new File(lexPath), "rw");
        FileChannel finalLexiconChannel = finalLexicon.getChannel();
        lexFinal.saveLexiconFinal(finalLexiconChannel);

        System.out.println("End building final Lexicon with TermUpperBound "+dtf.format(LocalDateTime.now()));
        System.out.println("Finish phase Indexing "+dtf.format(LocalDateTime.now()));

    }







}




