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
import java.util.TreeMap;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class Lexicon {


    TreeMap<Text,LexiconValue> lexicon;

    int currentIndex;

    public Lexicon(){
        lexicon = new TreeMap<>();
        this.currentIndex = 0;
    }

    public void addElement(Text term, long docId, InvertedIndex invInd){
        if(!lexicon.containsKey(term)){ // case new term
            LexiconValue lexiconValue = new LexiconValue(docId, currentIndex);
            invInd.addPostingOfNewTerm(docId);
            lexicon.put(term,lexiconValue);
            currentIndex += 1;
            lexiconValue.setLastDocument(docId);
        }
        else{ // case term already appeared in the same document
            if(lexicon.get(term).getLastDocument() == docId){
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                invInd.incrementPostingTF(lexicon.get(term).getIndex(), docId);
            }
            else{ // case term already appeared but in different document
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                invInd.addPostingOfExistingTerm(lexicon.get(term).getIndex(), docId);
                lexicon.get(term).setDf(lexicon.get(term).getDf() + 1);
                lexicon.get(term).setLastDocument(docId);
            }
        }
    }


    public void saveLexiconOnFile(String filePath) throws FileNotFoundException {
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

    public static LexiconValue transformByteToValue(byte[] value){
        LexiconValue lexValue = new LexiconValue(0,0);//Vedere che valori mettere

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

    public void clearLexicon(){
        this.lexicon.clear();
        this.lexicon = null;
        this.currentIndex = 0;
        System.gc();
    }

    public static LexiconLine readLexiconLine(String filePath,long startReadingPosition){
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
            buffer = ByteBuffer.allocate(32); //50 is the total number of bytes to read a complete term of the lexicon
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
        while (readingPositionFileLex1 < fcLex1.size() && readingPositionFileLex2 < fcLex2.size()) {
            Text t1 = readTermFromBlock(pathLex1, readingPositionFileLex1);
            Text t2 = readTermFromBlock(pathLex2, readingPositionFileLex2);

            if (t1.compareTo(t2) == 0) {
                LexiconLine lineLex1 = readLexiconLine(pathLex1, readingPositionFileLex1);
                LexiconLine lineLex2 = readLexiconLine(pathLex2, readingPositionFileLex2);
                LexiconLine lineLexMerge = new LexiconLine();
                lineLexMerge.setTerm(t1);
                lineLexMerge.setCf(lineLex1.getCf() + lineLex2.getCf());
                lineLexMerge.setDf(lineLex1.getDf() + lineLex2.getDf());

                //Fusione delle due posting con docids (la seconda va inserita alla fine della prima) Check per verificare
                byte[] byteArrayMergeDocId = Bytes.concat(InvertedIndex.readOneDocIdPostingList(lineLex1.getOffsetDocID(), pathDocID1, lineLex1.getDf()),
                        InvertedIndex.readOneDocIdPostingList(lineLex2.getOffsetDocID(), pathDocID2, lineLex2.getDf()));
                InvertedIndex.saveDocIdsOrTfsPostingLists(pathDocIDMerge,byteArrayMergeDocId,offsetDocIdMerge);

                lineLexMerge.setOffsetDocID(offsetDocIdMerge);

                //Fusione delle due Posting con tf
                byte[] byteArrayMergeTf = Bytes.concat(InvertedIndex.readOneTfsPostingList(lineLex1.getOffsetTF(), pathTF1, lineLex1.getDf()),
                        InvertedIndex.readOneTfsPostingList(lineLex2.getOffsetTF(),pathTF2,lineLex2.getDf()));
                InvertedIndex.saveDocIdsOrTfsPostingLists(pathTFMerge,byteArrayMergeTf,offsetTFMerge);
                lineLexMerge.setOffsetTF(offsetTFMerge);


                offsetDocIdMerge += byteArrayMergeDocId.length;
                offsetTFMerge += byteArrayMergeTf.length;
                lineLexMerge.setLenOfDocID(byteArrayMergeDocId.length);
                lineLexMerge.setLenOfTF(byteArrayMergeTf.length);

                lineLexMerge.saveLexiconLineOnFile(pathLexMerge,offsetFileLexMerge);
                readingPositionFileLex1 += 54;
                readingPositionFileLex2 += 54;
                offsetFileLexMerge += 54;

            }
            else if (t1.compareTo(t2) > 0) { // caso t1>t2
                LexiconLine lineLex = readLexiconLine(pathLex2, readingPositionFileLex2); //leggi lexiconLine

                InvertedIndex.saveDocIdsOrTfsPostingLists(pathDocIDMerge,
                        InvertedIndex.readOneDocIdPostingList(lineLex.getOffsetDocID(),pathDocID2, lineLex.getDf()),offsetDocIdMerge);
                lineLex.setOffsetDocID(offsetDocIdMerge);


                InvertedIndex.saveDocIdsOrTfsPostingLists(pathTFMerge,
                        InvertedIndex.readOneTfsPostingList(lineLex.getOffsetTF(), pathTF2, lineLex.getDf()),offsetTFMerge);
                lineLex.setOffsetTF(offsetTFMerge);

                lineLex.saveLexiconLineOnFile(pathLexMerge, offsetFileLexMerge);

                //Set degli elementi aggiornati
                offsetDocIdMerge += lineLex.getLenOffDocID();
                offsetTFMerge += lineLex.getLenOffTF();
                readingPositionFileLex2 += 54;
                offsetFileLexMerge += 54;

            } else { // caso t2>t1
                LexiconLine lineLex = readLexiconLine(pathLex1, readingPositionFileLex1);

                InvertedIndex.saveDocIdsOrTfsPostingLists(pathDocIDMerge,
                        InvertedIndex.readOneDocIdPostingList(lineLex.getOffsetDocID(),pathDocID1, lineLex.getDf()),offsetDocIdMerge);
                lineLex.setOffsetDocID(offsetDocIdMerge);

                InvertedIndex.saveDocIdsOrTfsPostingLists(pathTFMerge,
                        InvertedIndex.readOneTfsPostingList(lineLex.getOffsetTF(), pathTF1, lineLex.getDf()),offsetTFMerge);
                lineLex.setOffsetTF(offsetTFMerge);

                lineLex.saveLexiconLineOnFile(pathLexMerge, offsetFileLexMerge);

                offsetDocIdMerge += lineLex.getLenOffDocID();
                offsetTFMerge += lineLex.getLenOffTF();
                readingPositionFileLex1 += 54;
                offsetFileLexMerge += 54;
            }
        }
        while (readingPositionFileLex1 < fcLex1.size()) {
            LexiconLine lineLex = readLexiconLine(pathLex1, readingPositionFileLex1);

            InvertedIndex.saveDocIdsOrTfsPostingLists(pathDocIDMerge,
                    InvertedIndex.readOneDocIdPostingList(lineLex.getOffsetDocID(),pathDocID1, lineLex.getDf()),offsetDocIdMerge);
            lineLex.setOffsetDocID(offsetDocIdMerge);

            InvertedIndex.saveDocIdsOrTfsPostingLists(pathTFMerge,
                    InvertedIndex.readOneTfsPostingList(lineLex.getOffsetTF(), pathTF1, lineLex.getDf()),offsetTFMerge);
            lineLex.setOffsetTF(offsetTFMerge);

            lineLex.saveLexiconLineOnFile(pathLexMerge, offsetFileLexMerge);

            offsetDocIdMerge += lineLex.getLenOffDocID();
            offsetTFMerge += lineLex.getLenOffTF();
            readingPositionFileLex1 += 54;
            offsetFileLexMerge += 54;
        }
        while (readingPositionFileLex2 < fcLex2.size()) {
            LexiconLine lineLex = readLexiconLine(pathLex2, readingPositionFileLex2);

            InvertedIndex.saveDocIdsOrTfsPostingLists(pathDocIDMerge,
                    InvertedIndex.readOneDocIdPostingList(lineLex.getOffsetDocID(),pathDocID2, lineLex.getDf()),offsetDocIdMerge);
            lineLex.setOffsetDocID(offsetDocIdMerge);

            InvertedIndex.saveDocIdsOrTfsPostingLists(pathTFMerge,
                    InvertedIndex.readOneTfsPostingList(lineLex.getOffsetTF(), pathTF2, lineLex.getDf()), offsetTFMerge);
            lineLex.setOffsetTF(offsetTFMerge);

            lineLex.saveLexiconLineOnFile(pathLexMerge, offsetFileLexMerge);

            offsetDocIdMerge += lineLex.getLenOffDocID();
            offsetTFMerge += lineLex.getLenOffTF();
            readingPositionFileLex2 += 54;
            offsetFileLexMerge += 54;
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
        if(ReadingDocuments.nFileUsed > 2 ) {
            for (int i = 3; i <= ReadingDocuments.nFileUsed; i++) {
                mergeBlocks("Lexicon_Merge_number_" + (i - 2), "Lexicon_number_" + i, "Lexicon_Merge_number_" + (i - 1),
                        "Inverted_Index_Merge_DocId_number_" + (i - 2), "Inverted_Index_DocId_number_" + i, "Inverted_Index_Merge_DocId_number_" + (i - 1),
                        "Inverted_Index_Merge_TF_number_" + (i - 2), "Inverted_Index_TF_number_" + i, "Inverted_Index_Merge_TF_number_" + (i - 1));
            }
        }

    }


    public static Lexicon readAllLexicon(String filePath){
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        Lexicon lex = new Lexicon();

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

        InvertedIndex invInd = new InvertedIndex();
        Lexicon lex = new Lexicon();

        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);

        lex.addElement(new Text("a                   "), 1, invInd);

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


        InvertedIndex.saveDocIDsOnFile("DOCID1", lex);
        InvertedIndex.saveTFsOnFile("TF1", lex);
        lex.saveLexiconOnFile("LEX1");

        System.out.println("1: ------------------------------");
        for(Text term : lex.lexicon.keySet()){
            System.out.print(term + "  ");
            System.out.print("offsetTF: " + lex.lexicon.get(term).getOffsetTF() + "  ");
            System.out.print("offsetDocID " + lex.lexicon.get(term).getOffsetDocID() + "  ");
            System.out.print("length offsetDocID " + lex.lexicon.get(term).getLenOfDocID() + "  ");
            System.out.print("length offsetTF " + lex.lexicon.get(term).getLenOfTF() + "  ");
            System.out.print("CF: " + lex.lexicon.get(term).getCf() + "  ");
            System.out.print("DF: " + lex.lexicon.get(term).getDf() + "  ");

            for(int i=0; i<lex.lexicon.get(term).getDf(); i++){
                System.out.print("TF: " + invInd.allPostingLists.get(lex.lexicon.get(term).getIndex()).postingList + "  ");
            }
            System.out.print("\n");
        }
        System.out.print("\n");
        System.out.print("\n");
        System.out.print("\n");



        Lexicon lex2 = new Lexicon();
        InvertedIndex invInd2 = new InvertedIndex();

        lex2.addElement(new Text("d                   "), 1, invInd2);
        lex2.addElement(new Text("d                   "), 1, invInd2);
        lex2.addElement(new Text("d                   "), 1, invInd2);

        lex2.addElement(new Text("e                   "), 1, invInd2);

        lex2.addElement(new Text("f                   "), 1, invInd2);
        lex2.addElement(new Text("f                   "), 1, invInd2);
        lex2.addElement(new Text("f                   "), 1, invInd2);
        lex2.addElement(new Text("f                   "), 3, invInd2);
        lex2.addElement(new Text("f                   "), 500 , invInd2);


        lex2.addElement(new Text("g                   "), 1, invInd2);
        lex2.addElement(new Text("g                   "), 1, invInd2);
        lex2.addElement(new Text("g                   "), 1, invInd2);
        lex2.addElement(new Text("g                   "), 1, invInd2);
        lex2.addElement(new Text("g                   "), 1, invInd2);
        lex2.addElement(new Text("g                   "), 1, invInd2);
        lex2.addElement(new Text("g                   "), 1, invInd2);
        lex2.addElement(new Text("g                   "), 1, invInd2);
        lex2.addElement(new Text("g                   "), 1, invInd2);


        InvertedIndex.saveDocIDsOnFile("DOCID2", lex2);
        InvertedIndex.saveTFsOnFile("TF2", lex2);
        lex2.saveLexiconOnFile("LEX2");

        System.out.println("2: ------------------------------");
        for(Text term : lex2.lexicon.keySet()){
            System.out.print(term + "  ");
            System.out.print("offsetTF: " + lex2.lexicon.get(term).getOffsetTF() + "  ");
            System.out.print("offsetDocID " + lex2.lexicon.get(term).getOffsetDocID() + "  ");
            System.out.print("length offsetDocID " + lex2.lexicon.get(term).getLenOfDocID() + "  ");
            System.out.print("length offsetTF " + lex2.lexicon.get(term).getLenOfTF() + "  ");
            System.out.print("CF: " + lex2.lexicon.get(term).getCf() + "  ");
            System.out.print("DF: " + lex2.lexicon.get(term).getDf() + "  ");

            for(int i=0; i<lex2.lexicon.get(term).getDf(); i++){
                System.out.print("TF: " + invInd2.allPostingLists.get(lex2.lexicon.get(term).getIndex()).postingList + "  ");
            }
            System.out.print("\n");
        }
        System.out.print("\n");
        System.out.print("\n");
        System.out.print("\n");

        Lexicon.mergeBlocks("LEX1", "LEX2", "LEXMERGE1",
                "DOCID1", "DOCID2", "DOCIDMERGE1",
                "TF1", "TF2", "TFMERGE1");

        long offsetFileLexicon = 0;
        long offsetFileInvertedDocId = 0;
        long offsetFileInvertedTf = 0;
        long offsetFileSkipInfo = 0;
        Path fileLex = Paths.get("LEXMERGE1");
        FileChannel fcLex = FileChannel.open(fileLex, READ);
        for( int i = 0; i< fcLex.size();i= i+54) {

            ArrayList<Long> offsets = InvertedIndex.compression(offsetFileLexicon,"LEXMERGE1",
                    "DOCIDMERGE1",
                    "TFMERGE1", offsetFileInvertedDocId,
                    offsetFileInvertedTf,offsetFileSkipInfo);

            offsetFileLexicon += 54;
            offsetFileSkipInfo = offsets.get(0);
            offsetFileInvertedDocId = offsets.get(1);
            offsetFileInvertedTf = offsets.get(2);
        }




    }
}

