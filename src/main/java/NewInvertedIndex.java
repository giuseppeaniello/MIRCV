
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class NewInvertedIndex {

    int indexOfFile;
    List<newPosting> allPostingLists; //list in which we have all the posting lists of the terms in this blocks


    public NewInvertedIndex(int indexOfFile){
        this.indexOfFile = indexOfFile;
        this.allPostingLists = new ArrayList<newPosting>();
    }

    // case term appeared for the first time
    public void addPostingOfNewTerm(long currentOffset, long docID){ //add the first posting of a new posting list
        this.allPostingLists.add((int) currentOffset, new newPosting(docID));
    }

    // case term already appeared but in different document
    public void addPostingOfExistingTerm(long offset, long docID, long df){ //add a new posting in existing posting list
        this.allPostingLists.add((int) (offset + df), new newPosting(docID)); //we have offset+df to add the new posting at the end of the posting list
        //CONTROLLA CHE QUA SOPRA NON CI VADA TIPO OFFSET+DF+1 O OFFSET+DF-1
    }

    // case term already appeared in the same document
    public void incrementPostingDF(long offset, long docID, long df){
        for(int i=0; i<df; i++){ //look the entire posting list of this term //CONTROLLA CHE NON CI VADA TIPO i+1 O i-1
            if(this.allPostingLists.get((int)offset + i).getDocID() == docID){ //find the posting of the right document
                this.allPostingLists.get((int)offset + i).incrementDocumentFrequency(); //increment the TF of the posting of that document
                break;
            }
        }
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
        allPostingLists.clear();
        System.gc();
    }


    public byte[] compressListOfTFs(){
        // use unary compression
        int numOfBitsNecessary = 0;
        for (newPosting post : allPostingLists) { // Here we are looking for the number of bytes we will need for our compressed numbers
            int numOfByteNecessary = (int) (Math.floorDiv(post.getTF(), 8) + 1); // qui si può anche fare tutto con una variabile sola
            numOfBitsNecessary += (numOfByteNecessary * 8); // però diventa illeggibile quindi facciamolo alla fine
        }
        boolean[] result = new boolean[numOfBitsNecessary];
        int j = 0;
        for(newPosting post : allPostingLists){
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

     private boolean[] fromByteArrToBooleanArr(byte[] byteArray) {
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

   /* public byte[] compressionListOfDocId(){
        //Use Gamma Compression
        //sx -> Unary della dim dei bit che ci vogliono per conversione in binario
        //dx -> Concersione in binario meno cifra significativa
        for(newPosting post : allPostingLists){

        }
    }*/


    public static void main(String[] argv ){
/*
        //a=1/3 b=2/2 c=3/4 e=4/2 f=5/1 g=6/11
        newPosting a = new newPosting(1);
        a.incrementDocumentFrequency();
        a.incrementDocumentFrequency();
        newPosting b = new newPosting(2);
        b.incrementDocumentFrequency();
        newPosting c = new newPosting(3);
        c.incrementDocumentFrequency();
        c.incrementDocumentFrequency();
        c.incrementDocumentFrequency();
        newPosting e = new newPosting(4);
        e.incrementDocumentFrequency();
        newPosting f = new newPosting(5);
        newPosting g = new newPosting(6);
        g.incrementDocumentFrequency();
        g.incrementDocumentFrequency();
        g.incrementDocumentFrequency();
        g.incrementDocumentFrequency();
        g.incrementDocumentFrequency();
        g.incrementDocumentFrequency();
        g.incrementDocumentFrequency();
        g.incrementDocumentFrequency();
        g.incrementDocumentFrequency();
        g.incrementDocumentFrequency();


        NewInvertedIndex d = new NewInvertedIndex(0);
        d.allPostingLists.add(a);
        d.allPostingLists.add(b);
        d.allPostingLists.add(c);
        d.allPostingLists.add(e);
        d.allPostingLists.add(f);
        d.allPostingLists.add(g);

        byte[] compress = d.compressListOfTFs();
        String s = compress.toString();
        boolean[] pippo = d.fromByteArrToBooleanArr(compress);

        String str = "";
        for (boolean tmp : pippo){
            if(tmp == true)
                str += " 1 ";
            else
                str += " 0 ";
        }
        System.out.println(str);
 */


    }
}
