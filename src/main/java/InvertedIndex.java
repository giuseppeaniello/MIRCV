import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class InvertedIndex {

    static List<PostingList> allPostingLists;



    public InvertedIndex(){
        allPostingLists = new ArrayList<PostingList>();
    }

    // case term appeared for the first time
    public void addPostingOfNewTerm(long docID){ //add the first posting of a new posting list
        PostingList ps = new PostingList(docID);
        allPostingLists.add(ps);
    }

    // case term already appeared but in different document
    public void addPostingOfExistingTerm(long index, long docID){ //add a new posting in existing posting list
        allPostingLists.get((int) index).postingList.put(docID, 1);
    }

    // case term already appeared in the same document
    public void incrementPostingTF(long index, long docID){
        allPostingLists.get((int) index).incrementTF(docID); //increment the TF of the posting of that document
    }


    public void clearInvertedIndex(){
        this.allPostingLists.clear();
        this.allPostingLists = null;
        System.gc();
    }


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




    public static byte[] readDocIDsOrTFsPostingListCompressed(String filePath,long startReadingPosition, int lenOffeset){
        byte[] result = new byte[lenOffeset];
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        try (FileChannel fc = FileChannel.open(fileP, READ)) {
            fc.position(startReadingPosition);
            buffer = ByteBuffer.allocate(lenOffeset);
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            result = buffer.array();
            buffer.clear();
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return result;
    }


    private static String binaryWhitoutMostSignificant(long docID){
        return Long.toBinaryString(docID).substring(1); // convert docID in binary and trash first element
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
            if(boolArray[i] == false){ // quando trova il primo 0 vuol dire che il numero è finito
                listOfTFs.add(count); // lo aggiunge alla lista, adesso bisogna riprendere dal byte successivo
                i = i + ( 8*(int)( Math.floor(count/8) +1 ) ) - count; // in questo modo si riparte dall'inizio del byte successivo (in realtà dal bit prima ma poi appena ricomincia in for fa i++)
                count = 0;
            }
        }
        return listOfTFs;
    }

    public static ArrayList<Long> decompressionListOfDocIds(byte[] compression){ //// DA TESTARE
        ArrayList<Long> result = new ArrayList<>();
        boolean[] boolArray = fromByteArrToBooleanArr(compression);
        int count = 0; //tengo il conto della posizione a cui sono arrivato a guardare
        while(count < boolArray.length) {
            int leftPart = 1; // leftPart incrementa di 1 finchè non incontro uno 0
            for (int i = 0; i < boolArray.length; i++) {
                if (boolArray[count] == false) {
                    count++; //salto lo zero finale della unary
                    break;
                }
                else{
                    leftPart++;
                    count++;
                }
            } //arrivato qui ho la prima parte
            // ora vogliamo decodificare la seconda parte
            String str = "1"; // nella compressione togliamo il bit più significativo quindi qua lo riaggiungo
            for (int i=0; i<(leftPart -1); i++) { //creo una stringa in cui ho la codifica binaria della seconda parte
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
            //ora allineiamo a byte (se ho già trovato la codifica non serve che andiamo avanti a leggere fino al byte dopo
            while( (count % 8) != 0 )
                count++;
        }
        return result;
    }




    public static void saveDocIDsOnFile(String filePath, Lexicon lex) throws FileNotFoundException {
        int offset = 0;
        RandomAccessFile fileTf = new RandomAccessFile(filePath ,"rw");
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        for(Text term:lex.lexicon.keySet()){
            lex.lexicon.get(term).setOffsetDocID(offset);
            lex.lexicon.get(term).setLenOfDocID(lex.lexicon.get(term).getDf()*8);
            byte[] listOfDocIDs = allPostingLists.get( lex.lexicon.get(term).getIndex() ).convertDocIDsToByteArray();
            try (FileChannel fc = FileChannel.open(fileP, WRITE)) {
                fc.position(offset);
                buffer = ByteBuffer.wrap(listOfDocIDs);
                while (buffer.hasRemaining()) {
                    fc.write(buffer);
                }
                buffer.clear();
                offset += lex.lexicon.get(term).getLenOfDocID();
            } catch (IOException ex) {
                System.err.println("I/O Error: " + ex);
            }
        }
    }

    public static void saveTFsOnFile(String filePath, Lexicon lex) throws FileNotFoundException {
        int offset = 0;
        RandomAccessFile fileTf = new RandomAccessFile(filePath ,"rw");
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        for(Text term:lex.lexicon.keySet()){
            lex.lexicon.get(term).setOffsetTF(offset);
            lex.lexicon.get(term).setLenOfTF(lex.lexicon.get(term).getDf()*4);
            byte[] listOfTFs = allPostingLists.get( lex.lexicon.get(term).getIndex() ).convertTFsToByteArray();
            try (FileChannel fc = FileChannel.open(fileP, WRITE)) {
                fc.position(offset);
                buffer = ByteBuffer.wrap(listOfTFs);
                while (buffer.hasRemaining()) {
                    fc.write(buffer);
                }
                buffer.clear();
                offset += lex.lexicon.get(term).getLenOfTF();
            } catch (IOException ex) {
                System.err.println("I/O Error: " + ex);
            }
        }
    }

    public static void saveDocIdsOrTfsPostingLists(String filePath, byte[] listPosting,long startingPoint) throws FileNotFoundException {

        RandomAccessFile fileTf = new RandomAccessFile(filePath ,"rw");
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;

        try (FileChannel fc = FileChannel.open(fileP, WRITE)) {
            fc.position(startingPoint);
            buffer = ByteBuffer.wrap(listPosting);
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
    }

    public static byte[] readOneDocIdPostingList(long startReadingPosition, String filePath, int df) {
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        byte[] resultByte = new byte[df*8];
        try (FileChannel fc = FileChannel.open(fileP, READ))
        {
            fc.position(startReadingPosition);
            buffer = ByteBuffer.allocate(df*8); //50 is the total number of bytes to read a complete term of the lexicon
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            resultByte = buffer.array();

            buffer.clear();
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return resultByte;
    }

    public static byte[] transformArrayLongToByteArray(ArrayList<Long> array){
        byte[] converted;
        ByteBuffer bb = ByteBuffer.allocate(array.size()*8);
        for (Long tmp : array)
            bb.putLong(tmp);

        converted=bb.array();
        return converted;
    }
    public static byte[] transformArrayIntToByteArray(ArrayList<Integer> array){
        byte[] converted;
        ByteBuffer bb = ByteBuffer.allocate(array.size()*4);
        for (Integer tmp : array)
            bb.putLong(tmp);

        converted=bb.array();
        return converted;
    }
    public static byte[] readOneTfsPostingList(long startReadingPosition, String filePath, int df) {
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        byte[] resultByte = new byte[df*4];
        try (FileChannel fc = FileChannel.open(fileP, READ))
        {
            fc.position(startReadingPosition);
            buffer = ByteBuffer.allocate(df*4);
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            resultByte = buffer.array();

            buffer.clear();
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return resultByte;
    }
    public static ArrayList<Long> compression(long startLexiconLine,String pathLexMerge,String pathInvDocIds,String pathInvTfs,
                                   long offsetInvDocids, long offsetInvTFs, long offsetSkipInfo,long offsetLexSkip) throws FileNotFoundException {
        //Open Lexicon to retrieve offset where is saved the PostingList
        LexiconLine line = Lexicon.readLexiconLine(pathLexMerge,startLexiconLine);

        //Save in Array elements that need to be compressed
        ArrayList<Long> postingDocIds;
        ArrayList<Integer> postingTfs;

        postingDocIds =transformByteToLongArray(readOneDocIdPostingList(line.getOffsetDocID(),pathInvDocIds,line.getDf()));
        postingTfs = transformByteToIntegerArray(readOneTfsPostingList(line.getOffsetTF(),pathInvTfs,line.getDf()));

        //Calculate numbers of block for the skipInfo
        int nBlocks = (int) Math.ceil(Math.sqrt(postingDocIds.size()));
        line.setnBlock(nBlocks);
        // questa mi sa che deve essere così perchè facendo floor + 1 nel caso in cui il numero viene tondo conta un blocco di troppo ***************************************
        int sizeBlock = (int) Math.ceil(Math.sqrt(postingDocIds.size()));
        ArrayList<Long> dGapArray = new ArrayList<>();
        ArrayList<Integer> tfArray = new ArrayList<>();
        long lastDoc=0; //Used to calculate dGap
        int currentBlock = 1;

        int lastCompressionLengthDocIds = 0;
        int lastCompressionLengthTfs = 0;

        //Compression of posting List
        for(int i =0 ; i<postingDocIds.size();i++){
            //Condizioni DA CAMBIAREEEEEE
            if(i != postingDocIds.size()-1 && (i+1)%sizeBlock!=0){
                //Build of a single block
                tfArray.add(postingTfs.get(i));
                dGapArray.add(postingDocIds.get(i) - lastDoc);
                lastDoc = postingDocIds.get(i);
            }

            else if(i == postingDocIds.size()-1 || (i+1)%sizeBlock==0 ){   //Saving of the single block
                //Add last element of block
                dGapArray.add(postingDocIds.get(i) - lastDoc);
                tfArray.add(postingTfs.get(i));
                System.out.println("DGAP: "+ dGapArray);
                System.out.println("TF: "+tfArray);
                lastDoc=0;
                //Insert all the values of the skip and procede with the saving
                SkipInfo infoBlock = new SkipInfo();
                infoBlock.setFinalDocId(postingDocIds.get(i));
                //Compression Tfs Array
                byte[] compressedTfArray = compressListOfTFs(tfArray);
                InvertedIndex.saveDocIdsOrTfsPostingLists("InvertedTF",compressedTfArray,offsetInvTFs);
                infoBlock.setLenBlockTf(compressedTfArray.length);
                //Compression DGap Array
                byte[] compressedDGap = compressListOfDocIDs(dGapArray);
                InvertedIndex.saveDocIdsOrTfsPostingLists("InvertedDocId",compressedDGap,offsetInvDocids);
                infoBlock.setLenBlockDocId(compressedDGap.length);

                if(currentBlock == 1)
                    line.setOffsetSkipBlocks(offsetSkipInfo);
                infoBlock.setOffsetDocId(offsetInvDocids); //Offset inserito punta al valore all'interno dell'InvertedIndex
                infoBlock.setOffsetTf(offsetInvTFs);

                infoBlock.saveSkipInfoBlock("SkipInfo",offsetSkipInfo, infoBlock.trasformInfoToByte());

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
        line.saveLexiconLineWithSkip("Lexicon",offsetLexSkip);
        offsetLexSkip += 42;
        ArrayList<Long> offsets = new ArrayList<>();
        offsets.add(offsetSkipInfo);
        offsets.add(offsetInvDocids);
        offsets.add(offsetInvTFs);
        offsets.add(offsetLexSkip);
        System.out.println("FINEEE");
        return offsets;
    }

//Funzione per leggere su un file
    public static byte[] readFromAtoB(int start, String filePath, int end) {
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        byte[] resultByte = new byte[end-start];
        try (FileChannel fc = FileChannel.open(fileP, READ))
        {
            fc.position(start);
            buffer = ByteBuffer.allocate(end-start); //50 is the total number of bytes to read a complete term of the lexicon
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            resultByte = buffer.array();

            buffer.clear();
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return resultByte;
    }
    public static void main(String[] argv ) throws IOException {
        //Funzioni SkipInfo ok
        //Funzione save compress and decompress Docid ok
        //
        //compression(long startLexiconLine,String pathLexMerge,String pathInvDocIds,String pathInvTfs,
        //                                   long offsetInvDocids, long offsetInvTFs, long offssetSkipInfo)

       // ArrayList<Long> a = compression(54,"Lexicon_Merge_number_6","Inverted_Index_Merge_DocId_number_6",
         //      "Inverted_Index_Merge_TF_number_6",0,0,0);
/*
        InvertedIndex invInd = new InvertedIndex();
        InvertedIndex invInd1 = new InvertedIndex();
        InvertedIndex invInd2 = new InvertedIndex();
        Lexicon lex = new Lexicon();

        lex.addElement(new Text("b                   "), 4, invInd);
        lex.addElement(new Text("b                   "), 4, invInd);
        lex.addElement(new Text("b                   "), 4, invInd);
        lex.addElement(new Text("b                   "), 4, invInd);
        lex.addElement(new Text("b                   "), 4, invInd);

        lex.addElement(new Text("b                   "), 5, invInd);
        lex.addElement(new Text("b                   "), 5, invInd);
        lex.addElement(new Text("b                   "), 5, invInd);

        lex.addElement(new Text("b                   "), 7, invInd);
        lex.addElement(new Text("b                   "), 7, invInd);
        lex.addElement(new Text("b                   "), 7, invInd);
        lex.addElement(new Text("b                   "), 7, invInd);
        lex.addElement(new Text("b                   "), 7, invInd);
        lex.addElement(new Text("b                   "), 7, invInd);
        lex.addElement(new Text("b                   "), 7, invInd);
        lex.addElement(new Text("b                   "), 7, invInd);

        lex.addElement(new Text("b                   "),11,invInd);

        lex.addElement(new Text("b                   "),14,invInd);
        lex.addElement(new Text("b                   "),14,invInd);

        lex.addElement(new Text("bb                  "),15,invInd1);

        lex.addElement(new Text("b                   "),16,invInd);
        lex.addElement(new Text("b                   "),16,invInd);
        lex.addElement(new Text("b                   "),16,invInd);
        lex.addElement(new Text("b                   "),16,invInd);
        lex.addElement(new Text("b                   "),16,invInd);
        lex.addElement(new Text("b                   "),16,invInd);
        lex.addElement(new Text("b                   "),16,invInd);
        lex.addElement(new Text("b                   "),16,invInd);


        lex.addElement(new Text("b                   "),17,invInd);

        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);
        lex.addElement(new Text("b                   "),18,invInd);

        lex.addElement(new Text("b                   "),19,invInd);

        lex.addElement(new Text("b                   "),25,invInd);
        lex.addElement(new Text("b                   "),25,invInd);
        lex.addElement(new Text("b                   "),25,invInd);

        lex.addElement(new Text("bb                  "),25,invInd1);
        lex.addElement(new Text("bb                  "),25,invInd1);
        lex.addElement(new Text("bb                  "),25,invInd1);
        lex.addElement(new Text("bb                  "),47,invInd1);
        lex.addElement(new Text("a                   "),50,invInd2);


        saveDocIDsOnFile("INVDOC",lex);
        saveTFsOnFile("INVTF",lex);
        lex.saveLexiconOnFile("LEX");

   //     ArrayList<Long> aaa = compression(0,"LEX","INVDOC","INVTF",0,0,0);


        LexiconLine l ;

        l = LexiconLine.readLexiconLineSkip("LEX",0);
        l.printLexiconLine();

/*
        SkipInfo info = SkipInfo.readSkipInfoFromFile("SkipInfo",0);

        byte[] a = readDocIDsOrTFsPostingListCompressed("InvertedTF",info.getOffsetTf(), info.getLenBlockTf());

        System.out.println(decompressionListOfTfs(a));
*/

        LexiconLine l;

        l = LexiconLine.readLexiconLineSkip("Lexicon",42*2);
        l.printLexiconLineWithSkip();

        SkipInfo info = SkipInfo.readSkipInfoFromFile("SkipInfo",l.getOffsetSkipBlocks()+32);
        info.printSkipInfo();

        byte[] a = readDocIDsOrTFsPostingListCompressed("InvertedDocId",info.getoffsetDocId(), info.getLenBlockDocId());
        System.out.println(decompressionListOfDocIds(a));

        byte[] b = readDocIDsOrTFsPostingListCompressed("InvertedTF",info.getOffsetTf(), info.getLenBlockTf());
        System.out.println(decompressionListOfTfs(b));



    }
}
