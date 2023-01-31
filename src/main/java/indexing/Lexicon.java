package indexing;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.io.Text;
import queryProcessing.SkipBlock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.TreeMap;

public class Lexicon {

    TreeMap<Text,LexiconValue> lexicon;
    int currentIndex;

    public Lexicon(){
        lexicon = new TreeMap<>();
        this.currentIndex = 0;
    }

    // method to add a term in indexing.Lexicon
    public void addElement(Text term, long docId, InvertedIndex invInd){
       // case new term
        if(!lexicon.containsKey(term)){
            // creation of new lexiconValue to be added
            LexiconValue lexiconValue = new LexiconValue(docId, currentIndex);
            // adding the first posting to the new posting list of the new term
            invInd.addPostingOfNewTerm(docId);
            lexicon.put(term,lexiconValue);
            currentIndex += 1;
            // set last document to keep track of what documents have already been processed
            // in order to know when we have same term in same document
            lexiconValue.setLastDocument(docId);
        }
        else{ // case term already appeared in the same document
            if(lexicon.get(term).getLastDocument() == docId){
                // collection frequency is incremented
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                // term frequency is incremented
                invInd.incrementPostingTF(lexicon.get(term).getIndex(), docId);
            }
            else{ // case term already appeared but in different document
                // collection frequency is incremented
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                // creation of new posting
                invInd.addPostingOfExistingTerm(lexicon.get(term).getIndex(), docId);
                // document frequency is incremented
                lexicon.get(term).setDf(lexicon.get(term).getDf() + 1);
                // set last document to keep track of what documents have already been processed
                // in order to know when we have same term in same document
                lexicon.get(term).setLastDocument(docId);
            }
        }
    }

    // method to save on file the lexicon
    public void saveLexiconOnFile(FileChannel fc) throws IOException {
        ByteBuffer buffer = null;
        for(Text key : lexicon.keySet()){
            buffer = ByteBuffer.wrap(key.getBytes());
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
            // transformation in byte of all the values of a term
            byte[] valueByte = transformValueToByte(lexicon.get(key).getCf(), lexicon.get(key).getDf(),
                    lexicon.get(key).getOffsetDocID(),
                    lexicon.get(key).getOffsetTF(),
                    lexicon.get(key).getLenOfDocID(),
                    lexicon.get(key).getLenOfTF());
            buffer = ByteBuffer.wrap(valueByte);
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
        }



    }

    // method to convert all the field of a indexing.LexiconValue in byte[]
    public static byte[] transformValueToByte( int cF, int dF, long offsetDocId, long offsetTF, int lenOfDocID,int lenOfTF ) {
        ByteBuffer bb = ByteBuffer.allocate(32);
        bb.putInt(cF);
        bb.putInt(dF);
        bb.putLong(offsetDocId);
        bb.putLong(offsetTF);
        bb.putInt(lenOfDocID);
        bb.putInt(lenOfTF);
        return bb.array();
    }

    // method to read all the field of a indexing.LexiconValue from byte[]
    public static LexiconValue transformByteToValue(byte[] value){
        LexiconValue lexValue = new LexiconValue(0,0);
        int count = 0;
        for (byte b : value) {
            if(count<4)
                lexValue.setCf((lexValue.getCf() << 8) + (b & 0xFF));
            else if(count <8 && count>=4)
                lexValue.setDf((lexValue.getDf() << 8) + (b & 0xFF));
            else if(count <16 && count>=8)
                lexValue.setOffsetDocID((lexValue.getOffsetDocID() << 8) + (b & 0xFF));
            else if(count<24 && count >= 16)
                lexValue.setOffsetTF((lexValue.getOffsetTF() << 8) + (b & 0xFF));
            else if(count<28 && count >= 24)
                lexValue.setLenOfDocID((lexValue.getLenOfDocID() << 8) + (b & 0xFF));
            else
                lexValue.setLenOfTF((lexValue.getLenOfTF() << 8) + (b & 0xFF));
            count ++;
        }
        return lexValue;
    }

    // method to clean a lexicon and call garbage collection
    public void clearLexicon(){
        this.lexicon.clear();
        this.lexicon = null;
        this.currentIndex = 0;
        System.gc();
    }

    // method to read a single lexicon line from file given the offset in which this line starts on file
    public static LexiconLine readLexiconLine(FileChannel fc,long startReadingPosition) throws IOException {
        ByteBuffer buffer = null;
        LexiconLine lexVal = new LexiconLine();
        // reading of the term
        fc.position(startReadingPosition);
        buffer = ByteBuffer.allocate(22);
        do {
            fc.read(buffer);
        } while (buffer.hasRemaining());
        lexVal.setTerm(new Text(buffer.array()));
        buffer.clear();
        // reading of the values
        fc.position(startReadingPosition+22);
        buffer = ByteBuffer.allocate(32); //5 is the total number of bytes to read a complete term of the lexicon
        do {
            fc.read(buffer);
        } while (buffer.hasRemaining());
        // setting all the read values in the indexing.LexiconLine to be returned
        LexiconValue values = transformByteToValue(buffer.array());
        buffer.clear();
        lexVal.setCf(values.getCf());
        lexVal.setDf(values.getDf());
        lexVal.setOffsetTF(values.getOffsetTF());
        lexVal.setOffsetDocID(values.getOffsetDocID());
        lexVal.setLenOfDocID(values.getLenOfDocID());
        lexVal.setLenOfTF(values.getLenOfTF());

        return lexVal;
    }

    // method to read a term from a block of indexing.Lexicon during SPIMI
    public static Text readTermFromBlock(FileChannel fc, int startReadingPosition) throws IOException {
        ByteBuffer buffer = null;
        Text term = null;
        fc.position(startReadingPosition);
        buffer = ByteBuffer.allocate(22);
        do {
            fc.read(buffer);
        } while (buffer.hasRemaining());
        term = new Text(buffer.array());
        buffer.clear();
        return term;
    }

    // method to merge all the blocks created during SPIMI in order to obtain a single indexing.Lexicon and a single indexing.InvertedIndex
    // this merge is performed two files at a time until there is just one file left
      public static void mergeBlocks(FileChannel fcLex1, FileChannel fcLex2, FileChannel fcLexMerge, FileChannel fcDocID1,
                                   FileChannel fcDocID2, FileChannel fcDocIDMerge, FileChannel fcTF1, FileChannel fcTF2,
                                   FileChannel fcTFMerge) throws IOException {
        // offset to read on file1
        int readingPositionFileLex1 = 0;
        // offset to read on file2
        int readingPositionFileLex2 = 0;
        // offset to write on file merge
        long offsetFileLexMerge = 0;
        // offset to write docIds in file merge
        long offsetDocIdMerge = 0;
        // offset to write TFs in file merge
        long offsetTFMerge = 0;

        // while neither file1 or file2 has completely been read
        while (readingPositionFileLex1 < fcLex1.size() && readingPositionFileLex2 < fcLex2.size()) {
            // read the two terms
            Text t1 = readTermFromBlock(fcLex1, readingPositionFileLex1);
            Text t2 = readTermFromBlock(fcLex2, readingPositionFileLex2);
            // if the two terms are the same term
            if (t1.compareTo(t2) == 0) {
                LexiconLine lineLex1 = readLexiconLine(fcLex1, readingPositionFileLex1);
                LexiconLine lineLex2 = readLexiconLine(fcLex2, readingPositionFileLex2);
                LexiconLine lineLexMerge = new LexiconLine();
                lineLexMerge.setTerm(t1);
                // the two CFs are summed
                lineLexMerge.setCf(lineLex1.getCf() + lineLex2.getCf());
                // the two DFs are summed
                lineLexMerge.setDf(lineLex1.getDf() + lineLex2.getDf());
                //
                //Fusione delle due posting con docids (la seconda va inserita alla fine della prima) Check per verificare
                byte[] byteArrayMergeDocId = Bytes.concat(InvertedIndex.readOneDocIdPostingList(lineLex1.getOffsetDocID(), fcDocID1, lineLex1.getDf()),
                        InvertedIndex.readOneDocIdPostingList(lineLex2.getOffsetDocID(), fcDocID2, lineLex2.getDf()));
                InvertedIndex.saveDocIdsOrTfsPostingLists(fcDocIDMerge,byteArrayMergeDocId,offsetDocIdMerge);
                lineLexMerge.setOffsetDocID(offsetDocIdMerge);
                //Fusione delle due Posting con tf
                byte[] byteArrayMergeTf = Bytes.concat(InvertedIndex.readOneTfsPostingList(lineLex1.getOffsetTF(), fcTF1, lineLex1.getDf()),
                        InvertedIndex.readOneTfsPostingList(lineLex2.getOffsetTF(),fcTF2,lineLex2.getDf()));
                InvertedIndex.saveDocIdsOrTfsPostingLists(fcTFMerge,byteArrayMergeTf,offsetTFMerge);
                lineLexMerge.setOffsetTF(offsetTFMerge);
                offsetDocIdMerge += byteArrayMergeDocId.length;
                offsetTFMerge += byteArrayMergeTf.length;
                lineLexMerge.setLenOfDocID(byteArrayMergeDocId.length);
                lineLexMerge.setLenOfTF(byteArrayMergeTf.length);
                lineLexMerge.saveLexiconLineOnFile(fcLexMerge,offsetFileLexMerge);
                readingPositionFileLex1 += 54;
                readingPositionFileLex2 += 54;
                offsetFileLexMerge += 54;
            }
            else if (t1.compareTo(t2) > 0) { // caso t1>t2
                LexiconLine lineLex = readLexiconLine(fcLex2, readingPositionFileLex2); //leggi lexiconLine

                InvertedIndex.saveDocIdsOrTfsPostingLists(fcDocIDMerge,
                        InvertedIndex.readOneDocIdPostingList(lineLex.getOffsetDocID(),fcDocID2, lineLex.getDf()),offsetDocIdMerge);
                lineLex.setOffsetDocID(offsetDocIdMerge);

                InvertedIndex.saveDocIdsOrTfsPostingLists(fcTFMerge,
                        InvertedIndex.readOneTfsPostingList(lineLex.getOffsetTF(), fcTF2, lineLex.getDf()),offsetTFMerge);
                lineLex.setOffsetTF(offsetTFMerge);

                lineLex.saveLexiconLineOnFile(fcLexMerge, offsetFileLexMerge);
                //Set degli elementi aggiornati
                offsetDocIdMerge += lineLex.getLenOffDocID();
                offsetTFMerge += lineLex.getLenOffTF();
                readingPositionFileLex2 += 54;
                offsetFileLexMerge += 54;
            } else { // caso t2>t1
                LexiconLine lineLex = readLexiconLine(fcLex1, readingPositionFileLex1);

                InvertedIndex.saveDocIdsOrTfsPostingLists(fcDocIDMerge,
                        InvertedIndex.readOneDocIdPostingList(lineLex.getOffsetDocID(),fcDocID1, lineLex.getDf()),offsetDocIdMerge);
                lineLex.setOffsetDocID(offsetDocIdMerge);

                InvertedIndex.saveDocIdsOrTfsPostingLists(fcTFMerge,
                        InvertedIndex.readOneTfsPostingList(lineLex.getOffsetTF(), fcTF1, lineLex.getDf()),offsetTFMerge);
                lineLex.setOffsetTF(offsetTFMerge);

                lineLex.saveLexiconLineOnFile(fcLexMerge, offsetFileLexMerge);

                offsetDocIdMerge += lineLex.getLenOffDocID();
                offsetTFMerge += lineLex.getLenOffTF();
                readingPositionFileLex1 += 54;
                offsetFileLexMerge += 54;
            }
        }
        while (readingPositionFileLex1 < fcLex1.size()) {
            LexiconLine lineLex = readLexiconLine(fcLex1, readingPositionFileLex1);

            InvertedIndex.saveDocIdsOrTfsPostingLists(fcDocIDMerge,
                    InvertedIndex.readOneDocIdPostingList(lineLex.getOffsetDocID(),fcDocID1, lineLex.getDf()),offsetDocIdMerge);
            lineLex.setOffsetDocID(offsetDocIdMerge);

            InvertedIndex.saveDocIdsOrTfsPostingLists(fcTFMerge,
                    InvertedIndex.readOneTfsPostingList(lineLex.getOffsetTF(), fcTF1, lineLex.getDf()),offsetTFMerge);
            lineLex.setOffsetTF(offsetTFMerge);

            lineLex.saveLexiconLineOnFile(fcLexMerge, offsetFileLexMerge);

            offsetDocIdMerge += lineLex.getLenOffDocID();
            offsetTFMerge += lineLex.getLenOffTF();
            readingPositionFileLex1 += 54;
            offsetFileLexMerge += 54;
        }
        while (readingPositionFileLex2 < fcLex2.size()) {
            LexiconLine lineLex = readLexiconLine(fcLex2, readingPositionFileLex2);

            InvertedIndex.saveDocIdsOrTfsPostingLists(fcDocIDMerge,
                    InvertedIndex.readOneDocIdPostingList(lineLex.getOffsetDocID(),fcDocID2, lineLex.getDf()),offsetDocIdMerge);
            lineLex.setOffsetDocID(offsetDocIdMerge);

            InvertedIndex.saveDocIdsOrTfsPostingLists(fcTFMerge,
                    InvertedIndex.readOneTfsPostingList(lineLex.getOffsetTF(), fcTF2, lineLex.getDf()), offsetTFMerge);
            lineLex.setOffsetTF(offsetTFMerge);

            lineLex.saveLexiconLineOnFile(fcLexMerge, offsetFileLexMerge);

            offsetDocIdMerge += lineLex.getLenOffDocID();
            offsetTFMerge += lineLex.getLenOffTF();
            readingPositionFileLex2 += 54;
            offsetFileLexMerge += 54;
        }

    }

    public static void deleteFile(String path) {
        try {
            Files.deleteIfExists(Paths.get(path));
        } catch (NoSuchFileException e) {
            System.out.println("No such file/directory exists");
        } catch (DirectoryNotEmptyException e) {
            System.out.println("Directory is not empty.");
        } catch (IOException e) {
            System.out.println("Invalid permissions.");
            e.printStackTrace();
        }
    }

    public static  void mergeAllBlocks() throws IOException {
        //Open all fileChannel
        RandomAccessFile lexFile1 = new RandomAccessFile(new File("Lexicon_number_"+1), "r");
        FileChannel lexChannel1 = lexFile1.getChannel();
        RandomAccessFile lexFile2 = new RandomAccessFile(new File("Lexicon_number_"+2), "r");
        FileChannel lexChannel2 = lexFile2.getChannel();
        RandomAccessFile invertedDocIdFile1 = new RandomAccessFile(new File("Inverted_Index_DocId_number_" + 1), "r");
        FileChannel invDocIdsChannel1 = invertedDocIdFile1.getChannel();
        RandomAccessFile invertedDocIdFile2 = new RandomAccessFile(new File("Inverted_Index_DocId_number_" + 2), "r");
        FileChannel invDocIdsChannel2 = invertedDocIdFile2.getChannel();
        RandomAccessFile invTfsFile1 = new RandomAccessFile(new File("Inverted_Index_TF_number_" + 1), "r");
        FileChannel invertedTfsChannel1 = invTfsFile1.getChannel();
        RandomAccessFile invTfsFile2 = new RandomAccessFile(new File("Inverted_Index_TF_number_" + 2), "r");
        FileChannel invertedTfsChannel2 = invTfsFile2.getChannel();
        //File Merge
        RandomAccessFile lexFileMerge = new RandomAccessFile(new File("Lexicon_Merge_number_1"), "rw");
        FileChannel lexChannelMerge = lexFileMerge.getChannel();
        RandomAccessFile invDocIdFileMerge = new RandomAccessFile(new File("Inverted_Index_Merge_DocId_number_1"), "rw");
        FileChannel invDocIdsChannelMerge = invDocIdFileMerge.getChannel();
        RandomAccessFile invTfsFileMerge = new RandomAccessFile(new File("Inverted_Index_Merge_TF_number_1"), "rw");
        FileChannel invertedTfsChannelMerge = invTfsFileMerge.getChannel();
        //Start with merging of two first blocks
        mergeBlocks( lexChannel1, lexChannel2, lexChannelMerge,
                     invDocIdsChannel1, invDocIdsChannel2, invDocIdsChannelMerge,
                    invertedTfsChannel1, invertedTfsChannel2, invertedTfsChannelMerge);
        //Close all FileChannel
        lexChannel1.close();
        lexChannel2.close();
        lexChannelMerge.close();
        invDocIdsChannel1.close();
        invDocIdsChannel2.close();
        invDocIdsChannelMerge.close();
        invertedTfsChannel1.close();
        invertedTfsChannel2.close();
        invertedTfsChannelMerge.close();
        deleteFile("Lexicon_number_"+1);
        deleteFile("Lexicon_number_"+2);
        deleteFile("Inverted_Index_DocId_number_" + 1);
        deleteFile("Inverted_Index_DocId_number_" + 2);
        deleteFile("Inverted_Index_TF_number_" + 1);
        deleteFile("Inverted_Index_TF_number_" + 2);
        //Iteration of merging process
        if(Indexing.nFileUsed > 2 ) {
            for (int i = 3; i <= Indexing.nFileUsed; i++) {
                //Open all fileChannel
                lexFile1 = new RandomAccessFile(new File("Lexicon_Merge_number_"+(i-2)), "r");
                lexChannel1 = lexFile1.getChannel();
                lexFile2 = new RandomAccessFile(new File("Lexicon_number_"+(i)), "r");
                lexChannel2 = lexFile2.getChannel();
                invertedDocIdFile1 = new RandomAccessFile(new File("Inverted_Index_Merge_DocId_number_" + (i-2)), "r");
                invDocIdsChannel1 = invertedDocIdFile1.getChannel();
                invertedDocIdFile2 = new RandomAccessFile(new File("Inverted_Index_DocId_number_" + (i)), "r");
                invDocIdsChannel2 = invertedDocIdFile2.getChannel();
                invTfsFile1 = new RandomAccessFile(new File("Inverted_Index_Merge_TF_number_" + (i-2)), "r");
                invertedTfsChannel1 = invTfsFile1.getChannel();
                invTfsFile2 = new RandomAccessFile(new File("Inverted_Index_TF_number_" + (i)), "r");
                invertedTfsChannel2 = invTfsFile2.getChannel();
                //File Merge
                lexFileMerge = new RandomAccessFile(new File("Lexicon_Merge_number_"+(i-1)), "rw");
                lexChannelMerge = lexFileMerge.getChannel();
                invDocIdFileMerge = new RandomAccessFile(new File("Inverted_Index_Merge_DocId_number_"+(i-1)), "rw");
                invDocIdsChannelMerge = invDocIdFileMerge.getChannel();
                invTfsFileMerge = new RandomAccessFile(new File("Inverted_Index_Merge_TF_number_"+(i-1)), "rw");
                invertedTfsChannelMerge = invTfsFileMerge.getChannel();

                mergeBlocks( lexChannel1, lexChannel2, lexChannelMerge,
                        invDocIdsChannel1, invDocIdsChannel2, invDocIdsChannelMerge,
                        invertedTfsChannel1, invertedTfsChannel2, invertedTfsChannelMerge);
                lexChannel1.close();
                lexChannel2.close();
                lexChannelMerge.close();
                invDocIdsChannel1.close();
                invDocIdsChannel2.close();
                invDocIdsChannelMerge.close();
                invertedTfsChannel1.close();
                invertedTfsChannel2.close();
                invertedTfsChannelMerge.close();

                deleteFile("Lexicon_Merge_number_"+(i-2));
                deleteFile("Lexicon_number_"+i);
                deleteFile("Inverted_Index_Merge_DocId_number_" + (i-2));
                deleteFile("Inverted_Index_DocId_number_" + (i));
                deleteFile("Inverted_Index_Merge_TF_number_" + (i-2));
                deleteFile("Inverted_Index_TF_number_" + (i));
            }

        }
    }

    public  Ranking computeScoresForATermTfidfForUpperBound(Text term,FileChannel skipInfoFileChannel,
                                                            FileChannel invDocIdChannel,FileChannel invTfChannel) throws IOException {
        Ranking result = new Ranking();
        long tmpPosPosting = lexicon.get(term).getOffsetSkipBlocks();
        int nBlocks = lexicon.get(term).getnBlock();
        ArrayList<SkipBlock> info = new ArrayList<>();

        for (int i = 0; i < nBlocks; i++) {
            info.add(SkipBlock.readSkipBlockFromFile(skipInfoFileChannel, tmpPosPosting + (i * 32)));
        }
        for (int i = 0;i<nBlocks; i++) {
            ArrayList<Long> postingDocid = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                    invDocIdChannel, info.get(i).getoffsetDocId(), info.get(i).getLenBlockDocId())));
            ArrayList<Integer> postingTf = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                    invTfChannel , info.get(i).getOffsetTf(), info.get(i).getLenBlockTf()));
            result.calculateTFIDFScoreQueryTerm(postingDocid, postingTf, lexicon.get(term).getDf());
        }
        //indexing.InvertedIndex.decompressionListOfDocIds(indexing.InvertedIndex.readDocIDsOrTFsPostingListCompressed(invDocIdChannelAfterCompression,83051,5))
        //indexing.InvertedIndex.decompressionListOfTfs(indexing.InvertedIndex.readDocIDsOrTFsPostingListCompressed(invTFChannelAfterCompression,82246,1))
        return result;
    }

    public  Ranking computeScoresForATermBM25ForUpperBound(Text term, DocumentTable docTab, FileChannel skipInfoFileChannel,
                                                           FileChannel invDocIdChannel,FileChannel invTfChannel) throws IOException {
        Ranking result = new Ranking();
        long tmpPosPosting = lexicon.get(term).getOffsetSkipBlocks();
        int nBlocks = lexicon.get(term).getnBlock();
        ArrayList<SkipBlock> info = new ArrayList<>();

            for (int i = 0; i < nBlocks; i++) {
                info.add(SkipBlock.readSkipBlockFromFile(skipInfoFileChannel, tmpPosPosting + (i * 32)));
            }
            for (int i = 0; i < nBlocks; i++) {
                ArrayList<Long> postingDocid = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                        invDocIdChannel, info.get(i).getoffsetDocId(), info.get(i).getLenBlockDocId())));
                ArrayList<Integer> postingTf = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                        invTfChannel, info.get(i).getOffsetTf(), info.get(i).getLenBlockTf()));
                result.computeRSVbm25(postingDocid, postingTf, docTab, lexicon.get(term).getDf());
            }

        return result;
    }

    public static Lexicon readAllLexicon(FileChannel fc) throws IOException {

        ByteBuffer buffer = null;
        Lexicon lex = new Lexicon();

        for(int i = 0; i<fc.size(); i=i+42) {
            fc.position(i);
            buffer = ByteBuffer.allocate(22); //50 is the total number of bytes to read a complete term of the lexicon
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            Text term = new Text(buffer.array());
            buffer.clear();
            fc.position( i+ 22);
            buffer = ByteBuffer.allocate(20); //42 is the total number of bytes to read a complete term of the lexicon
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            LexiconLine value = LexiconLine.transformByteWIthSkipToLexicon(buffer.array());
            LexiconValue val = new LexiconValue();
            val.setnBlock(value.getnBlock());
            val.setOffsetSkipBlocks(value.getOffsetSkipBlocks());
            val.setCf(value.getCf());
            val.setDf(value.getDf());
            buffer.clear();

            lex.lexicon.put(term,val);
        }


        return lex;
    }



}

