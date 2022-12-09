import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class InvertedIndex {

    int indexOfFile;
    List<Posting> allPostingLists; //list in which we have all the posting lists of the terms in this blocks


    public InvertedIndex(int indexOfFile){
        this.indexOfFile = indexOfFile;
        this.allPostingLists = new ArrayList<Posting>();
    }

    // case term appeared for the first time
    public void addPostingOfNewTerm(long currentOffset, long docID){ //add the first posting of a new posting list
        this.allPostingLists.add((int) currentOffset, new Posting(docID));
    }

    // case term already appeared but in different document
    public void addPostingOfExistingTerm(long offset, long docID, long df){ //add a new posting in existing posting list
       // this.allPostingLists.add((int) (offset + df), new Posting(docID)); //we have offset+df to add the new posting at the end of the posting list
        this.allPostingLists.add(new Posting(docID) ); //we have offset+df to add the new posting at the end of the posting list

        //CONTROLLA CHE QUA SOPRA NON CI VADA TIPO OFFSET+DF+1 O OFFSET+DF-1
    }

    // case term already appeared in the same document
    public void incrementPostingTF(long offset, long docID, long df){
        this.allPostingLists.get((int)(offset + df - 1) ).incrementTermFrequency(); //increment the TF of the posting of that document (the last of the posting list)
    }

    public void saveInvertedIndexOnFile(){
        // prendi lista di docID
        // prendi lista TF
        // comprimi lista docID
        // salva lista docID compressi in un file
        // comprimi lista TF
        // salva lista TF compressa in un altro file
    }

    public void clearInvertedIndex(){
        this.allPostingLists.clear();
        this.allPostingLists = null;
        this.indexOfFile = 0;
        System.gc();
    }


    public byte[] compressListOfTFs(){
        // use unary compression
        int numOfBitsNecessary = 0;
        for (Posting post : allPostingLists) { // Here we are looking for the number of bytes we will need for our compressed numbers
            int numOfByteNecessary = (int) (Math.floor(post.getTF()/8) + 1); // qui si può anche fare tutto con una variabile sola
            numOfBitsNecessary += (numOfByteNecessary * 8); // però diventa illeggibile quindi facciamolo alla fine
        }
        boolean[] result = new boolean[numOfBitsNecessary];
        int j = 0;
        for(Posting post : allPostingLists){
           long zerosToAdd = 8 - (post.getTF() % 8); //number of zeros to be added to allign to byte each TF
            for(int i=0; i<post.getTF()-1; i++){
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

    public byte[] compressListOfDocIDsAndAssignOffsetsDocIDs(Lexicon lex){ //and assign offsetsDocIDs
        List<Boolean> result = new ArrayList<>(); // lista in cui mettiamo tutto via via, non posso usare array perchè non conosco dimensione
        int offsetDocIdCompressed = 0; // corrisponde all'offsetDocID
        boolean first = true; // per impostare l'offset del primo a 0;
        for (Text term : lex.lexicon.keySet()) { // prendo una parola
            int bitForThisTerm = 0; // per tenere il conto di quanti bit per questa parola
            long df = lex.lexicon.get(term).getDf(); // mi salvo la sua df
            if(first){ // se siamo al primo term ci salviamo offsetDocID 0
             lex.lexicon.get(term).setOffsetDocID(0);
             first = false;
            }
            else // se non siamo al primo term salviamo offsetDocID pari al numero di byte usati fin ora per la lista di docID compressi
                lex.lexicon.get(term).setOffsetDocID(offsetDocIdCompressed); // metto l'offsetDocID pari al numero di byte usato fin ora per la lista di docID compressi
            for (int j = 0; j < df; j++) { // prima di cambiare parola scorro la posting list della parola e accumulo i byte che uso per i docID compressi di quella posting list
                Posting post = allPostingLists.get((int) lex.lexicon.get(term).getOffsetInList() + j); // per scorrere tutta la posting list della parola
                int bitUsed = 0; // tengo il conto via via di quanti bit uso ad ogni ciclo
                String strRightPart = binaryWhitoutMostSignificant(post.getDocID()); //ottengo la parte destra
                for (int i = 0; i < strRightPart.length(); i++) { // aggiungo a result la parte sinistra cioè l'unary della dimensione di (strRightPart+1)
                    result.add(true); // qui vengono aggiunti gli 1 dell'unary
                    bitUsed++;
                    bitForThisTerm++;
                }
                result.add(false); // qui viene aggiunto lo 0 finale della compressione unary
                bitUsed++;
                bitForThisTerm++;
                for (int i=0; i<strRightPart.length(); i++) { // ora aggiungo la parte a destra leggendo la stringa che contiene già esattamente la parte destra
                    if (strRightPart.charAt(i) == '1') {
                        result.add(true);
                        bitUsed++;
                        bitForThisTerm++;
                    } else{
                        result.add(false);
                        bitUsed++;
                        bitForThisTerm++;
                    }
                }
                // now allign to the byte, add 0s until bitUsed % 8 == 0
                while ((bitUsed % 8) != 0) {
                    result.add(false);
                    bitUsed++;
                    bitForThisTerm++;
                }
                offsetDocIdCompressed += bitUsed / 8; //accumulo il numero di byte occupati per conoscere l'offset del termine successivo
            } // passiamo al prossimo posting della posting list del termine
            lex.lexicon.get(term).setLenOfDocID(bitForThisTerm/8); // save for this term the length of the compressed list of docIDs
        } // se abbiamo gia' scorso tutta la posting list allora passa alla parola dopo e la prima cosa che fa è salvare l'offset
        boolean[] arrBool = new boolean[result.size()]; //transform from list<bool> to bool[]
        for(int i=0; i<arrBool.length; i++)
            arrBool[i] = result.get(i);
        return fromBooleanArrToByteArr(arrBool);
    }

    public void readInvertedDocIds(String filePath,int startReadingPosition, int lenOffesetDocId){
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        ArrayList<Integer> decompressionValue = new ArrayList<>();
        try (FileChannel fc = FileChannel.open(fileP, READ))
        {
            fc.position(startReadingPosition);
            buffer = ByteBuffer.allocate(lenOffesetDocId);
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
           // decompressionValue = DocI(buffer.array()); ci vuole funzione decompressione docids
            buffer.clear();
            System.out.println(decompressionValue);
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
    }

    public void readInvertedTF(String filePath,int startReadingPosition, int lenOffesetTF){
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        ArrayList<Integer> decompressionValue = new ArrayList<>();
        try (FileChannel fc = FileChannel.open(fileP, READ))
        {
            fc.position(startReadingPosition);
            buffer = ByteBuffer.allocate(lenOffesetTF);
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());

            decompressionValue = decompressionListOfTfs(buffer.array());
            buffer.clear();
            System.out.println(decompressionValue);
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
    }

    private String binaryWhitoutMostSignificant(long docID){
        return Long.toBinaryString(docID).substring(1); // convert docID in binary and trash first element
    }

    private byte[] fromBooleanArrToByteArr(boolean[] boolArr){
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

     public boolean[] fromByteArrToBooleanArr(byte[] byteArray) {
        BitSet bits = BitSet.valueOf(byteArray);
        boolean[] bools = new boolean[byteArray.length * 8];
        for (int i = bits.nextSetBit(0); i != -1; i = bits.nextSetBit(i+1)) {
            bools[i] = true;
        }
        return bools;
    }

    public ArrayList<Integer> decompressionListOfTfs(byte[] compression){
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

    public ArrayList<Integer> decompressionListOfDocIds(byte[] compression){ //// DA TESTARE
        ArrayList<Integer> result = new ArrayList<>();
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
            for (int i=0; i<(leftPart - 1); i++) { //creo una stringa in cui ho la codifica binaria della seconda parte
                if (boolArray[count] == true) {
                    str += '1';
                    count++;
                }
                else {
                    str += '0';
                    count++;
                }
            }
            result.add(Integer.parseInt(str, 2));
            //ora allineiamo a byte (se ho già trovato la codifica non serve che andiamo avanti a leggere fino al byte dopo
            while( (count % 8) != 0 )
                count++;
        }
        return result;
    }




    public void saveTFCompressedOnFile(byte [] compressedTF,String filePath) throws FileNotFoundException {

        RandomAccessFile fileTf = new RandomAccessFile(filePath ,"rw");

        Path fileP = Paths.get(filePath );
        ByteBuffer buffer = null;

        try (FileChannel fc = FileChannel.open(fileP, WRITE)) {

            buffer = ByteBuffer.wrap(compressedTF);
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();

        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }

    }

    public void saveDocIdCompressedOnFile(byte [] compressedDocId,String filePath) throws FileNotFoundException {

        RandomAccessFile fileDocId = new RandomAccessFile(filePath ,"rw");

        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;

        try (FileChannel fc = FileChannel.open(fileP, WRITE)) {

            buffer = ByteBuffer.wrap(compressedDocId);
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();

        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }

    }

    public static void main(String[] argv ) throws IOException {

        //a=1/3 b=2/2 c=3/4 e=4/2 f=5/1 g=6/11
       /* newPosting a = new newPosting(1);
        a.incrementTermFrequency();
        a.incrementTermFrequency();
        newPosting b = new newPosting(2);
        b.incrementTermFrequency();
        newPosting c = new newPosting(3);
        c.incrementTermFrequency();
        c.incrementTermFrequency();
        c.incrementTermFrequency();
        newPosting e = new newPosting(4);
        e.incrementTermFrequency();
        newPosting f = new newPosting(5);
        newPosting g = new newPosting(6);
        g.incrementTermFrequency();
        g.incrementTermFrequency();
        g.incrementTermFrequency();
        g.incrementTermFrequency();
        g.incrementTermFrequency();
        g.incrementTermFrequency();
        g.incrementTermFrequency();
        g.incrementTermFrequency();
        g.incrementTermFrequency();
        g.incrementTermFrequency();


        NewInvertedIndex d = new NewInvertedIndex(0);
        d.allPostingLists.add(a);
        d.allPostingLists.add(b);
        d.allPostingLists.add(c);
        d.allPostingLists.add(e);
        d.allPostingLists.add(f);
        d.allPostingLists.add(g);

        byte[] compress = d.compressListOfTFs();

        /*
        String s = compress.toString();
        boolean[] pippo = d.fromByteArrToBooleanArr(compress);*/


        ArrayList<Integer> a = new ArrayList<>();
        a.add(6);
        a.add(5);
        a.add(99);
        System.out.println(a);




    }
}
