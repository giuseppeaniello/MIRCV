import com.google.common.primitives.Bytes;
import org.apache.commons.collections.ListUtils;
import org.apache.hadoop.io.Text;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class Lexicon {


    LinkedHashMap<Text,LexiconValue> lexicon;
    int indexOfFile;
    long currentOffset;

    public Lexicon(int indexOfFile){
        lexicon             = new LinkedHashMap<>();
        this.indexOfFile    = indexOfFile;
        this.currentOffset  = 0;
    }

    public void addElement(Text term, long docId, InvertedIndex invInd){
        if(!lexicon.containsKey(term)){ // case new term
            LexiconValue lexiconValue = new LexiconValue(currentOffset, docId);
            invInd.addPostingOfNewTerm(currentOffset, docId);
            lexicon.put(term,lexiconValue);
            currentOffset += 1;
            lexiconValue.setLastDocument(docId);
            updateAllOffsetsInList(term);
        }
        else{ // case term already appeared in the same document
            if(lexicon.get(term).getLastDocument() == docId){
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                invInd.incrementPostingTF(lexicon.get(term).getOffsetInList(), docId, lexicon.get(term).getDf());
            }
            else{ // case term already appeared but in different document
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                invInd.addPostingOfExistingTerm(lexicon.get(term).getOffsetInList(), docId-lexicon.get(term).getLastDocument(), lexicon.get(term).getDf());
                lexicon.get(term).setDf(lexicon.get(term).getDf() + 1);
                lexicon.get(term).setLastDocument(docId);
                currentOffset += 1;
                updateAllOffsetsInList(term);
            }
        }
    }

    public void sortLexicon(){
        //non va bene
        List<Text> sortedKeys = new ArrayList<>();
        for(Text term : lexicon.keySet())
            sortedKeys.add(term);

        sortedKeys.sort(null);
        LinkedHashMap<Text, LexiconValue> sortedLexicon = new LinkedHashMap<>();
        for(Text term : sortedKeys){
            sortedLexicon.put(term, lexicon.get(term));
            lexicon.remove(term);
        }
        lexicon = sortedLexicon;
        sortedLexicon = null;
        sortedKeys = null;
        System.gc();
    }

    public void saveLexiconOnFile(String filePath, int indexOfFile) throws FileNotFoundException {
        RandomAccessFile file = new RandomAccessFile(filePath ,"rw");
        Path fileP = Paths.get(filePath );
        ByteBuffer buffer = null;
        try (FileChannel fc = FileChannel.open(fileP, WRITE)) {
            for(Text key : lexicon.keySet()){
                buffer = ByteBuffer.wrap(key.getBytes());
                while (buffer.hasRemaining()) {
                    fc.write(buffer);
                }
                buffer.clear();
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
        } catch (IOException ex) {
        System.err.println("I/O Error: " + ex);
        }
    }

    public static byte[] transformValueToByte( int cF, long dF, long offsetDocId, long offsetTF, int lenOfDocID,int lenOfTF ) {
        ByteBuffer bb = ByteBuffer.allocate(36);
        bb.putInt(cF);
        bb.putLong(dF);
        bb.putLong(offsetDocId);
        bb.putLong(offsetTF);
        bb.putInt(lenOfDocID);
        bb.putInt(lenOfTF);
        return bb.array();
    }

    public static LexiconValue transformByteToValue(byte[] value){
        LexiconValue lexValue = new LexiconValue(0,0);//Vedere che valori mettere

        int count = 0;
        for (byte b : value) {
            if(count<4)
                lexValue.setCf((lexValue.getCf() << 8) + (b & 0xFF));
            else if(count <12 && count>=4)
                lexValue.setDf((lexValue.getDf() << 8) + (b & 0xFF));
            else if(count <20 && count>=12)
                lexValue.setOffsetDocID((lexValue.getOffsetDocID() << 8) + (b & 0xFF));
            else if(count<28 && count >= 20)
                lexValue.setOffsetTF((lexValue.getOffsetTF() << 8) + (b & 0xFF));
            else if(count<32 && count >= 28)
                lexValue.setLenOfDocID((lexValue.getLenOfDocID() << 8) + (b & 0xFF));
            else
                lexValue.setLenOfTF((lexValue.getLenOfTF() << 8) + (b & 0xFF));
            count ++;
        }

        return lexValue;
    }

    /*
    public void updateAllOffsetsInList(){ // questo non va chiamato, è già nel metodo che aggiunge gli elementi
        boolean first = true;  // non lo faccio private perchè magari dopo il merging ci serve anche chiamarlo a parte
        long offset = 0;
        long df = 0;
        for(Text term : lexicon.keySet()){
            if(first){
                df = lexicon.get(term).getDf();
                first = false;
            }
            else{
                lexicon.get(term).setOffsetInList(offset + df);
                df = lexicon.get(term).getDf();
                offset = lexicon.get(term).getOffsetInList();
            }
        }
    }
    */

    public void updateAllOffsetsInList(Text wordUpdated){
        boolean found = false;
        for(Text term : lexicon.keySet()){
            if(found == false) {
                if (wordUpdated == term)
                    found = true;
            }
            else{
                lexicon.get(term).setOffsetInList(lexicon.get(term).getOffsetInList()+1);
            }
        }
    }

    public void updateAllOffsetsTF(InvertedIndex invInd){ // to be done just one time before the sorting before the saving on file
        boolean first = true;
        long offsetTF = 0;
        int lenOffTF  = 0;
        long df = 0; // df of the previous term
        for(Text term : lexicon.keySet()){
            if(first){ //the first time the offsetTF is already 0
                df = lexicon.get(term).getDf();
                for(int i=0; i<df; i++){ // df indicates the number of posting, for each posting we will need a certain number of bytes
                    offsetTF += (long) Math.floor(invInd.allPostingLists.get((int)lexicon.get(term).getOffsetInList() + i).getTF() / 8)+1;
                }
                this.lexicon.get(term).setLenOfTF((int)offsetTF);
                first = false;
            }
            else{
                lenOffTF = 0;
                lexicon.get(term).setOffsetTF(offsetTF);
                df = lexicon.get(term).getDf();
                for(int i=0; i<df; i++){
                    lenOffTF += (long) Math.floor(invInd.allPostingLists.get((int)lexicon.get(term).getOffsetInList() + i).getTF() / 8) + 1;
                    // offsetTF += (long) Math.floor(invInd.allPostingLists.get((int)lexicon.get(term).getOffsetInList() + i).getTF() / 8) + 1;
                }
                offsetTF += lenOffTF;
                this.lexicon.get(term).setLenOfTF(lenOffTF);
            }
        }
    }


    public void clearLexicon(){
        this.lexicon.clear();
        this.lexicon = null;
        this.currentOffset = 0;
        this.indexOfFile = 0;
        System.gc();
    }

    public static LexiconLine readLexiconLine(String filePath,int startReadingPosition){
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        LexiconLine lexVal = new LexiconLine();
        try (FileChannel fc = FileChannel.open(fileP, READ))
        {
            fc.position(startReadingPosition);
            buffer = ByteBuffer.allocate(22); //50 is the total number of bytes to read a complete term of the lexicon
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            lexVal.setTerm(new Text(buffer.array()));
            buffer.clear();

            fc.position(startReadingPosition+22);
            buffer = ByteBuffer.allocate(36); //50 is the total number of bytes to read a complete term of the lexicon
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            LexiconValue values = transformByteToValue(buffer.array());
            buffer.clear();
            lexVal.setCf(values.getCf());
            lexVal.setDf(values.getDf());
            lexVal.setOffsetTF(values.getOffsetTF());
            lexVal.setOffsetDocID(values.getOffsetDocID());
            lexVal.setLenOfDocID(values.getLenOfDocID());
            lexVal.setLenOfTF(values.getLenOfTF());

        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return lexVal;
    }

    public static Text readTermFromBlock(String filePath, int startReadingPosition){
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        Text term = null;
        try (FileChannel fc = FileChannel.open(fileP, READ))
        {
            fc.position(startReadingPosition);
            buffer = ByteBuffer.allocate(22); //50 is the total number of bytes to read a complete term of the lexicon
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            term = new Text(buffer.array());
            buffer.clear();
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return term;
    }




    public static void mergeBlocks(String pathLex1, String pathLex2, String pathLexMerge, String pathDocID1, String pathDocID2, String pathDocIDMerge, String pathTF1, String pathTF2, String pathTFMerge) throws IOException {
        int readingPositionFileLex1 = 0;
        int readingPositionFileLex2 = 0;
        long offsetFileLexMerge = 0;
        long offsetDocIdMerge = 0;
        long offsetTFMerge = 0;
        Path fileLex1 = Paths.get(pathLex1);
        Path fileLex2 = Paths.get(pathLex2);
        FileChannel fcLex1 = FileChannel.open(fileLex1, READ);
        FileChannel fcLex2 = FileChannel.open(fileLex2, READ);
        while(readingPositionFileLex1 < fcLex1.size() && readingPositionFileLex2 < fcLex2.size()) {
            Text t1 = readTermFromBlock(pathLex1, readingPositionFileLex1);
            Text t2 = readTermFromBlock(pathLex2, readingPositionFileLex2);

            if (t1.compareTo(t2) == 0) {
                LexiconLine lineLex1 = readLexiconLine(pathLex1, readingPositionFileLex1);
                LexiconLine lineLex2 = readLexiconLine(pathLex2, readingPositionFileLex2);
                LexiconLine lineLexMerge = new LexiconLine();
                lineLexMerge.setTerm(t1);
                lineLexMerge.setCf(lineLex1.getCf() + lineLex2.getCf());
                lineLexMerge.setDf(lineLex1.getDf() + lineLex2.getDf());

                byte[] concatenationTF = Bytes.concat(InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathTF1,
                                lineLex1.getOffsetTF(), lineLex1.getLenOffTF()),
                        InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathTF2,
                                lineLex2.getOffsetTF(), lineLex2.getLenOffTF()));
                InvertedIndex.saveTForDocIDsCompressedOnFile(concatenationTF, pathTFMerge, offsetTFMerge);
                lineLexMerge.setOffsetTF(offsetTFMerge);
                offsetTFMerge += lineLex1.getLenOffTF() + lineLex2.getLenOffTF();
                lineLexMerge.setLenOfTF(lineLex1.getLenOffTF() + lineLex2.getLenOffTF());


                byte[] byteArray1 = InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathDocID1, lineLex1.getOffsetDocID(), lineLex1.getLenOffDocID());
                byte[] byteArray2 = InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathDocID2, lineLex2.getOffsetDocID(), lineLex2.getLenOffDocID());

                ArrayList<Long> list1 = InvertedIndex.decompressionListOfDocIds(byteArray1);
                ArrayList<Long> list2 = InvertedIndex.decompressionListOfDocIds(byteArray2);


                long sumAllDGap = InvertedIndex.sumDGap(list1);
                long firstValueSecondInvInd = list2.get(0);
                long newValue = firstValueSecondInvInd - sumAllDGap;
                list2.set(0, newValue);
                list1.addAll(list2);


                byte[] compressionDocID = InvertedIndex.compressListOfDocIDs(list1);
                InvertedIndex.saveTForDocIDsCompressedOnFile(compressionDocID, pathDocIDMerge, offsetDocIdMerge);
                lineLexMerge.setOffsetDocID(offsetDocIdMerge);
                offsetDocIdMerge += compressionDocID.length;
                lineLexMerge.setLenOfDocID(compressionDocID.length);
                lineLexMerge.saveLexiconLineOnFile(pathLexMerge, lineLexMerge, 1, offsetFileLexMerge);
                readingPositionFileLex1 += 58;
                readingPositionFileLex2 += 58;
                offsetFileLexMerge += 58;

            } else if (t1.compareTo(t2) > 0) { // caso t1>t2
                LexiconLine lineLex = readLexiconLine(pathLex2,readingPositionFileLex2); //leggi lexiconLine
//da qui
                InvertedIndex.saveTForDocIDsCompressedOnFile(InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathDocID2,
                        lineLex.getOffsetDocID(), lineLex.getLenOffDocID()), pathDocIDMerge, offsetDocIdMerge); // salvi i DocID
                // a qui
                lineLex.setOffsetDocID(offsetDocIdMerge);


                InvertedIndex.saveTForDocIDsCompressedOnFile(InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathTF2,
                        lineLex.getOffsetTF(), lineLex.getLenOffTF()), pathTFMerge, offsetTFMerge);
                lineLex.setOffsetTF(offsetTFMerge);


                lineLex.saveLexiconLineOnFile(pathLexMerge,lineLex,1,offsetFileLexMerge);


                offsetDocIdMerge += lineLex.getLenOffDocID();
                offsetTFMerge += lineLex.getLenOffTF();
                readingPositionFileLex2 +=58;
                offsetFileLexMerge += 58;

            } else { // caso t2>t1
                LexiconLine lineLex = readLexiconLine(pathLex1, readingPositionFileLex1);


                InvertedIndex.saveTForDocIDsCompressedOnFile(InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathDocID1,
                        lineLex.getOffsetDocID(), lineLex.getLenOffDocID()), pathDocIDMerge, offsetDocIdMerge);
                lineLex.setOffsetDocID(offsetDocIdMerge);


                InvertedIndex.saveTForDocIDsCompressedOnFile(InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathTF1,
                        lineLex.getOffsetTF(), lineLex.getLenOffTF()), pathTFMerge, offsetTFMerge);
                lineLex.setOffsetTF(offsetTFMerge);


                lineLex.saveLexiconLineOnFile(pathLexMerge,lineLex,1,offsetFileLexMerge);


                offsetDocIdMerge += lineLex.getLenOffDocID();
                offsetTFMerge += lineLex.getLenOffTF();
                readingPositionFileLex1 +=58;
                offsetFileLexMerge += 58;
            }
        }
        while(readingPositionFileLex1 < fcLex1.size()){
            LexiconLine lineLex = readLexiconLine(pathLex1,readingPositionFileLex1);

            InvertedIndex.saveTForDocIDsCompressedOnFile(InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathDocID1,
                    lineLex.getOffsetDocID(), lineLex.getLenOffDocID()), pathDocIDMerge, offsetDocIdMerge);

            InvertedIndex.saveTForDocIDsCompressedOnFile(InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathTF1,
                    lineLex.getOffsetTF(), lineLex.getLenOffTF()), pathTFMerge, offsetTFMerge);

            lineLex.setOffsetDocID(offsetDocIdMerge);
            lineLex.setOffsetTF(offsetTFMerge);
            lineLex.saveLexiconLineOnFile(pathLexMerge,lineLex,1,offsetFileLexMerge);

            offsetDocIdMerge += lineLex.getLenOffDocID();
            offsetTFMerge += lineLex.getLenOffTF();
            readingPositionFileLex1 +=58;
            offsetFileLexMerge += 58;
        }
        while(readingPositionFileLex2 < fcLex2.size()){
            LexiconLine lineLex = readLexiconLine(pathLex2,readingPositionFileLex2);

            InvertedIndex.saveTForDocIDsCompressedOnFile(InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathDocID2,
                    lineLex.getOffsetDocID(), lineLex.getLenOffDocID()), pathDocIDMerge, offsetDocIdMerge);

            InvertedIndex.saveTForDocIDsCompressedOnFile(InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathTF2,
                    lineLex.getOffsetTF(), lineLex.getLenOffTF()), pathTFMerge, offsetTFMerge);

            lineLex.setOffsetDocID(offsetDocIdMerge);
            lineLex.setOffsetTF(offsetTFMerge);
            lineLex.saveLexiconLineOnFile(pathLexMerge,lineLex,1,offsetFileLexMerge);

            offsetDocIdMerge += lineLex.getLenOffDocID();
            offsetTFMerge += lineLex.getLenOffTF();
            readingPositionFileLex2 += 58;
            offsetFileLexMerge += 58;
        }


        fcLex2.close();
        fcLex1.close();
        /*

        deleteFile(pathLex2);
        deleteFile(pathLex1);
        deleteFile(pathDocID1);
        deleteFile(pathDocID2);
        deleteFile(pathTF1);
        deleteFile(pathTF2);

         */
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

        mergeBlocks("Lexicon_number_1","Lexicon_number_2","Lexicon_Merge_number_1",
                "Inverted_Index_DocId_number_1","Inverted_Index_DocId_number_2","Inverted_Index_Merge_DocId_number_1",
                "Inverted_Index_TF_number_1","Inverted_Index_TF_number_2","Inverted_Index_Merge_TF_number_1");
        for (int i = 3; i<=ReadingDocuments.nFileUsed;i++){

            mergeBlocks("Lexicon_Merge_number_"+(i-2),"Lexicon_number_"+i,"Lexicon_Merge_number_"+(i-1),
                    "Inverted_Index_Merge_DocId_number_"+(i-2),"Inverted_Index_DocId_number_"+i,"Inverted_Index_Merge_DocId_number_"+(i-1),
                    "Inverted_Index_Merge_TF_number_"+(i-2),"Inverted_Index_TF_number_"+i,"Inverted_Index_Merge_TF_number_"+(i-1));
        }
    }
    public static Lexicon readAllLexicon(String filePath){
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        Lexicon lex = new Lexicon(0);

        try (FileChannel fc = FileChannel.open(fileP, READ))
        {

            for(int i = 0; i<fc.size(); i=i+58) {
                fc.position(i);
                buffer = ByteBuffer.allocate(22); //50 is the total number of bytes to read a complete term of the lexicon
                do {
                    fc.read(buffer);
                } while (buffer.hasRemaining());
                Text term = new Text(buffer.array());
                buffer.clear();

                fc.position( i+ 22);
                buffer = ByteBuffer.allocate(36); //50 is the total number of bytes to read a complete term of the lexicon
                do {
                    fc.read(buffer);
                } while (buffer.hasRemaining());
                LexiconValue value = transformByteToValue(buffer.array());
                buffer.clear();
                value.setCf(value.getCf());
                value.setDf(value.getDf());
                value.setOffsetTF(value.getOffsetTF());
                value.setOffsetDocID(value.getOffsetDocID());
                value.setLenOfDocID(value.getLenOfDocID());
                value.setLenOfTF(value.getLenOfTF());
                lex.lexicon.put(term,value);
            }

        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return lex;
    }


    public static void main (String[] arg) throws IOException {
/*
        Lexicon lex = new Lexicon(0);
        LexiconLine l = new LexiconLine();
        // l = readLexiconLine("Lexicon_number_1",0);
        // l.printLexiconLine();
        InvertedIndex invInd = new InvertedIndex(0);

        // 0,2,4
        lex.addElement(new Text("a                   "), 1, invInd);

        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 3, invInd);
        lex.addElement(new Text("b                   "), 500 , invInd);


        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);

        //lex.saveLexicon("prova", 0);
        lex.updateAllOffsetsInList();
        lex.updateAllOffsetsTF(invInd);
        invInd.compressListOfDocIDsAndAssignOffsetsDocIDs(lex);
        lex.sortLexicon();
       // lex.saveLexiconOnFile("LEX1",1);
        System.out.println("---------------------------------");
        for(Text term : lex.lexicon.keySet()){
            //prova anche a scorrere i posting
            System.out.print(term + "  ");
            System.out.print("offsetTF: " + lex.lexicon.get(term).getOffsetTF() + "  ");
            System.out.print("offsetList: " + lex.lexicon.get(term).getOffsetInList() + "  ");
            System.out.print("offsetDocID " + lex.lexicon.get(term).getOffsetDocID() + "  ");
            System.out.print("length offsetDocID " + lex.lexicon.get(term).getLenOfDocID() + "  ");
            System.out.print("length offsetTF " + lex.lexicon.get(term).getLenOfTF() + "  ");
            System.out.print("CF: " + lex.lexicon.get(term).getCf() + "  ");
            System.out.print("DF: " + lex.lexicon.get(term).getDf() + "  ");

            for(int i=0; i<lex.lexicon.get(term).getDf(); i++){
                System.out.print("docID: " + invInd.allPostingLists.get((int)lex.lexicon.get(term).getOffsetInList() + i).getDocID() + "  ");
                System.out.print("TF: " + invInd.allPostingLists.get((int)lex.lexicon.get(term).getOffsetInList() + i).getTF() + "  ");
            }
            System.out.print("\n");
        }
        System.out.print("\n");
        System.out.print("\n");
        System.out.print("\n");



        Lexicon lex2 = new Lexicon(0);
        InvertedIndex invInd2 = new InvertedIndex(0);

        lex2.addElement(new Text("b                   "), 501, invInd2);
        lex2.addElement(new Text("b                   "), 502, invInd2);
        lex2.addElement(new Text("b                   "), 502, invInd2);
        lex2.addElement(new Text("b                   "), 502, invInd2);
        lex2.addElement(new Text("b                   "), 502, invInd2);
        lex2.addElement(new Text("f                   "), 503, invInd2);
        lex2.addElement(new Text("b                   "), 505, invInd2);
        lex2.addElement(new Text("b                   "), 505 , invInd2);

        lex2.updateAllOffsetsInList();
        lex2.updateAllOffsetsTF(invInd2);
        invInd.compressListOfDocIDsAndAssignOffsetsDocIDs(lex2);
        lex2.sortLexicon();

        for(Text term : lex2.lexicon.keySet()){
            //prova anche a scorrere i posting
            System.out.print(term + "  ");
            System.out.print("offsetTF: " + lex2.lexicon.get(term).getOffsetTF() + "  ");
            System.out.print("offsetList: " + lex2.lexicon.get(term).getOffsetInList() + "  ");
            System.out.print("offsetDocID " + lex2.lexicon.get(term).getOffsetDocID() + "  ");
            System.out.print("length offsetDocID " + lex2.lexicon.get(term).getLenOfDocID() + "  ");
            System.out.print("length offsetTF " + lex2.lexicon.get(term).getLenOfTF() + "  ");
            System.out.print("CF: " + lex2.lexicon.get(term).getCf() + "  ");
            System.out.print("DF: " + lex2.lexicon.get(term).getDf() + "  ");

            for(int i=0; i<lex2.lexicon.get(term).getDf(); i++){
                System.out.print("docID: " + invInd.allPostingLists.get((int)lex2.lexicon.get(term).getOffsetInList() + i).getDocID() + "  ");
                System.out.print("TF: " + invInd.allPostingLists.get((int)lex2.lexicon.get(term).getOffsetInList() + i).getTF() + "  ");
            }
            System.out.print("\n");
        }

        lex.saveLexiconOnFile("LEX1", 1);
        lex2.saveLexiconOnFile("LEX2", 1);
        //lex.mergeBlocks("LEX1", "LEX2", "MERGED");


        LexiconLine line = new LexiconLine();
        line = readLexiconLine("MERGED", 58);
        line.printLexiconLine();
        */

/*
        Lexicon lex0 = new Lexicon(0);
        InvertedIndex invInd0 = new InvertedIndex(0);

        lex0.addElement(new Text("ciao                "), 1, invInd0);
        lex0.addElement(new Text("miao                "), 2, invInd0);
       // lex0.addElement(new Text("a                   "), 5, invInd0);
        lex0.addElement(new Text("miao                "), 2, invInd0);
        lex0.addElement(new Text("miao                "), 70, invInd0);
        lex0.addElement(new Text("z                "), 70, invInd0);



        lex0.updateAllOffsetsInList();
        lex0.updateAllOffsetsTF(invInd0);
        InvertedIndex.compressListOfDocIDsAndAssignOffsetsDocIDs(lex0);

        invInd0.saveTForDocIDsCompressedOnFile(invInd0.compressListOfDocIDsAndAssignOffsetsDocIDs(lex0), "DOCID"+0, 0 );
        invInd0.saveTForDocIDsCompressedOnFile(invInd0.compressListOfTFs(), "TF"+0, 0);
        lex0.sortLexicon();
        lex0.saveLexiconOnFile("LEX"+0, 0);


        Lexicon lex1 = new Lexicon(1);
        InvertedIndex invInd1 = new InvertedIndex(1);
        lex1.addElement(new Text("ciao                "), 78, invInd1);
      //  lex1.addElement(new Text("miao                "), 75, invInd1);
        lex1.addElement(new Text("ciao                "), 80, invInd1);
        lex1.addElement(new Text("miao                "), 81, invInd1);
        lex1.addElement(new Text("miao                "), 99, invInd1);

        lex1.updateAllOffsetsInList();
        lex1.updateAllOffsetsTF(invInd1);
        InvertedIndex.compressListOfDocIDsAndAssignOffsetsDocIDs(lex1);
        invInd1.saveTForDocIDsCompressedOnFile(invInd1.compressListOfDocIDsAndAssignOffsetsDocIDs(lex1), "DOCID"+1, 0 );
        invInd1.saveTForDocIDsCompressedOnFile(invInd1.compressListOfTFs(), "TF"+1, 0);
        lex1.sortLexicon();
        lex1.saveLexiconOnFile("LEX"+1, 0);


        Lexicon lex = new Lexicon(0);
*/
        /*Lexicon lex = new Lexicon(0);
        lex.mergeBlocks("Lexicon_number_1", "Lexicon_number_2", "LEXMERGE",
                "Inverted_Index_DocID_number_1", "Inverted_Index_DocID_number_2", "DOCIDMERGE",
                "Inverted_Index_TF_number_1", "Inverted_Index_TF_number_2", "TFMERGE");

        Lexicon.deleteFile("Inverted_Index_DocID_number_1");
        Lexicon.deleteFile("Inverted_Index_DocID_number_2");
        Lexicon.deleteFile("Inverted_Index_TF_number_1");
        Lexicon.deleteFile("Inverted_Index_TF_number_2");
        Lexicon.deleteFile("Lexicon_number_1");
        Lexicon.deleteFile("Lexicon_number_2");
/*
        LexiconLine lexLine = new LexiconLine();
        lexLine = Lexicon.readLexiconLine("LEXMERGE", 58);
        lexLine.printLexiconLine();

        byte[]bytes = InvertedIndex.readDocIDsOrTFsPostingListCompressed("TFMERGE", lexLine.getOffsetTF(), lexLine.getLenOffTF());
        System.out.println(InvertedIndex.decompressionListOfDocIds(bytes));



*/
        LexiconLine lineLex = new LexiconLine();
        System.out.println("LEX1");
        for(int i=0; i<58*20; i+=58) {
            lineLex = Lexicon.readLexiconLine("Lexicon_number_1", i);
            lineLex.printLexiconLine();
        }

        System.out.println("LEX2");
        for(int i=0; i<58*20; i+=58) {
            lineLex = Lexicon.readLexiconLine("Lexicon_number_2", i);
            lineLex.printLexiconLine();
        }

        System.out.println("LEX3");
        for(int i=0; i<58*20; i+=58) {
            lineLex = Lexicon.readLexiconLine("Lexicon_number_3", i);
            lineLex.printLexiconLine();
        }


        System.out.println("LEXMERGE");
        for(int i=0; i<58*20; i+=58) {
            lineLex = Lexicon.readLexiconLine("Lexicon_Merge_number_2", i);
            lineLex.printLexiconLine();
        }
    }


}
