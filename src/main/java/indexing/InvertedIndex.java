package indexing;

import org.apache.hadoop.io.Text;
import queryProcessing.SkipBlock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class InvertedIndex {
    //It is a list that contains all the posting list for each term
    static List<PostingList> allPostingLists;
    public InvertedIndex(){
        allPostingLists = new ArrayList<PostingList>();
    }
    //Case term appeared for the first time
    public void addPostingOfNewTerm(long docID){ //add the first posting of a new posting list
        PostingList ps = new PostingList(docID);
        allPostingLists.add(ps);
    }

    //Case term already appeared but in different document
    public void addPostingOfExistingTerm(long index, long docID){
        //add a new posting in existing posting list
        allPostingLists.get((int) index).postingList.put(docID, 1);
    }

    //Case term already appeared in the same document
    public void incrementPostingTF(long index, long docID){
        allPostingLists.get((int) index).incrementTF(docID); //increment the TF of the posting of that document
    }

    //Clean of inverted index used when a block is built
    public void clearInvertedIndex(){
        this.allPostingLists.clear();
        this.allPostingLists = null;
        System.gc();
    }

    //Unary compression of tfs
    public static byte[] compressListOfTFs(ArrayList<Integer> array){
        // use unary compression
        int numOfBitsNecessary = 0;
        for (int tf : array) { // Here we are looking for the number of bytes we will need for our compressed numbers
            int numOfByteNecessary = (int) (Math.floor(tf/8) + 1); // qui si può anche fare tutto con una variabile sola
            numOfBitsNecessary += (numOfByteNecessary * 8); // però diventa illeggibile quindi facciamolo alla fine
        }
        boolean[] result = new boolean[numOfBitsNecessary];
        int j = 0;
        for(int tf : array){
           long zerosToAdd = 8 - (tf % 8); //number of zeros to be added to allign to byte each TF
            for(int i=0; i<tf -1; i++){
                result[j] = true;
                j++;
            }
            // add zeros to allign to byte+
            for (int i=0; i<zerosToAdd; i++){
                j++; // instead of adding zeros we skip positions which are inizialized to false
            }
            j++;
        }
        return fromBooleanArrToByteArr(result);
    }

    //Gamma compression of D-gap
    public static byte[] compressListOfDocIDs(ArrayList<Long> list){
        List<Boolean> result = new ArrayList<>(); // lista in cui mettiamo tutto via via, non posso usare array perchè non conosco dimensione
        for(long elem : list) {
            int bitUsed = 0;
            String rightPart = binaryWhitoutMostSignificant(elem);
            for (int i = 0; i < rightPart.length(); i++) {
                result.add(true);
                bitUsed++;
            }
            result.add(false);
            bitUsed++;
            for (int i = 0; i < rightPart.length(); i++) { // ora aggiungo la parte a destra leggendo la stringa che contiene già esattamente la parte destra
                if (rightPart.charAt(i) == '1') {
                    result.add(true);
                    bitUsed++;
                }
                else {
                    result.add(false);
                    bitUsed++;
                }
            }
            while(bitUsed % 8 != 0){
                result.add(false);
                bitUsed++;
            }
        }
        boolean[] arrBool = new boolean[result.size()]; //transform from list<bool> to bool[]
        for(int i=0; i<arrBool.length; i++)
            arrBool[i] = result.get(i);
        return fromBooleanArrToByteArr(arrBool);
    }

    // method exploited in compressListOfDocIds()
    private static String binaryWhitoutMostSignificant(long docID){
        return Long.toBinaryString(docID).substring(1); // convert docID in binary and trash first element
    }

    //Function to read from file a compressed Posting list
    public static byte[] readDocIDsOrTFsPostingListCompressed(FileChannel fc,long startReadingPosition, int lenOffeset) throws IOException {
        byte[] result = new byte[lenOffeset];
        ByteBuffer buffer = null;
        fc.position(startReadingPosition);
        buffer = ByteBuffer.allocate(lenOffeset);
        do {
            fc.read(buffer);
        } while (buffer.hasRemaining());
        result = buffer.array();
        buffer.clear();
        return result;
    }

    public static ArrayList<Long> transformByteToLongArray(byte[] value){
        ArrayList<Long> convertArray= new ArrayList<>();

        ByteBuffer bb = ByteBuffer.wrap(value);
        for(int i =0; i<value.length/8;i++) {
             convertArray.add(bb.getLong());
        }
        return convertArray;
    }

    public static ArrayList<Integer> transformByteToIntegerArray(byte[] value){
        ArrayList<Integer> convertArray= new ArrayList<>();
        ByteBuffer bb = ByteBuffer.wrap(value);
        for(int i =0; i<value.length/4;i++) {
            convertArray.add(bb.getInt());
        }
        return convertArray;
    }

    private static byte[] fromBooleanArrToByteArr(boolean[] boolArr){
        BitSet bits = new BitSet(boolArr.length);
        for (int i = 0; i < boolArr.length; i++) {
            if (boolArr[i]) {
                bits.set(i);
            }
        }
        byte[] bytes = bits.toByteArray();
        if (bytes.length * 8 >= boolArr.length) {
            return bytes;
        } else {
            return Arrays.copyOf(bytes, boolArr.length / 8 + (boolArr.length % 8 == 0 ? 0 : 1));
        }
    }

     public static boolean[] fromByteArrToBooleanArr(byte[] byteArray) {
        BitSet bits = BitSet.valueOf(byteArray);
        boolean[] bools = new boolean[byteArray.length * 8];
        for (int i = bits.nextSetBit(0); i != -1; i = bits.nextSetBit(i+1)) {
            bools[i] = true;
        }
        return bools;
    }

    public static ArrayList<Integer> decompressionListOfTfs(byte[] compression){
        ArrayList<Integer> listOfTFs = new ArrayList<>();
        boolean[] boolArray = fromByteArrToBooleanArr(compression);
        int count = 0;
        for(int i=0; i<boolArray.length; i++){
            count++;
            // when first 0 is found means the number is finished
            if(boolArray[i] == false){
                // add it to the list, now restart from next byte
                listOfTFs.add(count);
                // ho to starting point of next byte (actually one bit before but then there is i++)
                i = i + ( 8*(int)( Math.floor(count/8) +1 ) ) - count;
                count = 0;
            }
        }
        return listOfTFs;
    }

    public static ArrayList<Long> decompressionListOfDocIds(byte[] compression){
        ArrayList<Long> result = new ArrayList<>();
        boolean[] boolArray = fromByteArrToBooleanArr(compression);
        // keep count of last position watched
        int count = 0;
        while(count < boolArray.length) {
            // increase left part of 1 while a 0 is encountered
            int leftPart = 1;
            for (int i = 0; i < boolArray.length; i++) {
                if (boolArray[count] == false) {
                    // jump the final 0 of unary compression
                    count++;
                    break;
                }
                else{
                    leftPart++;
                    count++;
                }
            } // here first part is completed
            // decode second part
            // add most significant byte removed during the compression
            String str = "1";
            // create string with binary encoding of second part
            for (int i=0; i<(leftPart -1); i++) {
                if (boolArray[count] == true) {
                    str += '1';
                    count++;
                }
                else {
                    str += '0';
                    count++;
                }
            }
            result.add(Long.parseLong(str, 2));
            // alligning to byte
            while( (count % 8) != 0 )
                count++;
        }
        return result;
    }

    // method to save list of DocIds on file
    public static void saveDocIDsOnFile(Lexicon lex, FileChannel fc) throws IOException {
        int offset = 0;
        ByteBuffer buffer = null;
        for(Text term:lex.lexicon.keySet()){
            lex.lexicon.get(term).setOffsetDocID(offset);
            lex.lexicon.get(term).setLenOfDocID(lex.lexicon.get(term).getDf()*8);
            byte[] listOfDocIDs = allPostingLists.get( lex.lexicon.get(term).getIndex() ).convertDocIDsToByteArray();
            fc.position(offset);
            buffer = ByteBuffer.wrap(listOfDocIDs);
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
            offset += lex.lexicon.get(term).getLenOfDocID();
        }
    }

    // method to save list of TFs on file
    public static void saveTFsOnFile(Lexicon lex, FileChannel fc) throws IOException {
        int offset = 0;
        ByteBuffer buffer = null;
        for(Text term:lex.lexicon.keySet()){
            lex.lexicon.get(term).setOffsetTF(offset);
            lex.lexicon.get(term).setLenOfTF(lex.lexicon.get(term).getDf()*4);
            byte[] listOfTFs = allPostingLists.get( lex.lexicon.get(term).getIndex() ).convertTFsToByteArray();
            fc.position(offset);
            buffer = ByteBuffer.wrap(listOfTFs);
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
            offset += lex.lexicon.get(term).getLenOfTF();
        }
    }

    public static void saveDocIdsOrTfsPostingLists(FileChannel fc, byte[] listPosting,long startingPoint) throws IOException {
        ByteBuffer buffer = null;
        fc.position(startingPoint);
        buffer = ByteBuffer.wrap(listPosting);
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        buffer.clear();
    }

    // method to convert docIds in D-gaps
    public static ArrayList<Long> trasformDgapInDocIds(ArrayList<Long> dgap){
        ArrayList<Long> result = new ArrayList<>();
        long sum = 0;

        for (int i = 0; i<dgap.size();i++){
            sum += dgap.get(i);
            result.add(sum);
        }
        return result;
    }

    // method to read docId posting list of a term
    public static byte[] readOneDocIdPostingList(long startReadingPosition, FileChannel fc, int df) throws IOException {
        ByteBuffer buffer = null;
        byte[] resultByte = new byte[df*8];
        fc.position(startReadingPosition);
        buffer = ByteBuffer.allocate(df*8);
        do {
            fc.read(buffer);
        } while (buffer.hasRemaining());
        resultByte = buffer.array();
        buffer.clear();
        return resultByte;
    }

    // method to read TF posting list of a term
    public static byte[] readOneTfsPostingList(long startReadingPosition, FileChannel fc, int df) throws IOException {
        ByteBuffer buffer = null;
        byte[] resultByte;
        fc.position(startReadingPosition);
        buffer = ByteBuffer.allocate(df*4);
        do {
            fc.read(buffer);
        } while (buffer.hasRemaining());
        resultByte = buffer.array();
        buffer.clear();
        return resultByte;
    }

    // method to perform compression of docId posting lists and TFs posting lists. It also performs Skip Info creation
   public static ArrayList<Long> compression(long startLexiconLine,FileChannel fcLexMerge,FileChannel fcInvDocIds,FileChannel fcInvTfs,
                                             long offsetInvDocids, long offsetInvTFs, long offsetSkipInfo, long offsetLexSkip,
                                             FileChannel lexAfterCompressionChannel, FileChannel invDocIdAfterCompressionChannel,
                                             FileChannel invTfAfterCompressionChannel, FileChannel skipInfoChannel) throws IOException {
        //Open indexing.Lexicon to retrieve offset where is saved the indexing.PostingList
        LexiconLine line = Lexicon.readLexiconLine(fcLexMerge,startLexiconLine);
        //Save in Array elements that need to be compressed
        ArrayList<Long> postingDocIds;
        ArrayList<Integer> postingTfs;
        postingDocIds =transformByteToLongArray(readOneDocIdPostingList(line.getOffsetDocID(),fcInvDocIds,line.getDf()));
        postingTfs = transformByteToIntegerArray(readOneTfsPostingList(line.getOffsetTF(),fcInvTfs,line.getDf()));
        //Calculate numbers of block for the skipInfo
        int sizeBlock = (int) Math.ceil(Math.sqrt(postingDocIds.size()));
        ArrayList<Long> dGapArray = new ArrayList<>();
        ArrayList<Integer> tfArray = new ArrayList<>();
        long lastDoc=0; //Used to calculate dGap
        int currentBlock = 0;
        //Compression of posting List
        for(int i =0 ; i<postingDocIds.size();i++){
            if(i != postingDocIds.size()-1 && (i+1)%sizeBlock!=0){
                //Build of a single block
                tfArray.add(postingTfs.get(i));
                dGapArray.add(postingDocIds.get(i) - lastDoc);
                lastDoc = postingDocIds.get(i);
            }
            //Saving of the single block
            else if(i == postingDocIds.size()-1 || (i+1)%sizeBlock==0 ){
                //Add last element of block
                dGapArray.add(postingDocIds.get(i) - lastDoc);
                tfArray.add(postingTfs.get(i));
                lastDoc=0;
                //Insert all the values of the skip and procede with the saving
                SkipBlock infoBlock = new SkipBlock();
                infoBlock.setFinalDocId(postingDocIds.get(i));
                //Compression Tfs Array
                byte[] compressedTfArray = compressListOfTFs(tfArray);

                InvertedIndex.saveDocIdsOrTfsPostingLists(invTfAfterCompressionChannel, compressedTfArray, offsetInvTFs);
                infoBlock.setLenBlockTf(compressedTfArray.length);
                //Compression DGap Array
                byte[] compressedDGap = compressListOfDocIDs(dGapArray);
                InvertedIndex.saveDocIdsOrTfsPostingLists(invDocIdAfterCompressionChannel, compressedDGap, offsetInvDocids);
                infoBlock.setLenBlockDocId(compressedDGap.length);
                //Set of the first block
                if(currentBlock == 0)
                    line.setOffsetSkipBlocks(offsetSkipInfo);
                //Offset inserted in the skipBlock
                infoBlock.setOffsetDocId(offsetInvDocids);
                infoBlock.setOffsetTf(offsetInvTFs);
                infoBlock.saveSkipInfoBlock(skipInfoChannel, offsetSkipInfo, infoBlock.trasformInfoToByte());
                //Update all variables
                currentBlock++;
                offsetSkipInfo += 32; // 32 = vedi in classe skipinfo
                offsetInvDocids += infoBlock.getLenBlockDocId();
                offsetInvTFs += infoBlock.getLenBlockTf();
                dGapArray.clear();
                tfArray.clear();
            }
            else{
                System.out.println("Something Wrong in the compression");
            }
        }
        line.setnBlock(currentBlock);
        //Save new indexing.Lexicon with skipInfo
        line.saveLexiconLineWithSkip(lexAfterCompressionChannel,offsetLexSkip);
        offsetLexSkip += 42;
        ArrayList<Long> offsets = new ArrayList<>();
        offsets.add(offsetSkipInfo);
        offsets.add(offsetInvDocids);
        offsets.add(offsetInvTFs);
        offsets.add(offsetLexSkip);

        return offsets;
    }


}
