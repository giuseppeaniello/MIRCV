
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

public class NewInvertedIndex {

    int indexOfFile;
    List<NewPosting> allPostingLists; //list in which we have all the posting lists of the terms in this blocks


    public NewInvertedIndex(int indexOfFile){
        this.indexOfFile = indexOfFile;
        this.allPostingLists = new ArrayList<NewPosting>();
    }

    // case term appeared for the first time
    public void addPostingOfNewTerm(long currentOffset, long docID){ //add the first posting of a new posting list
        this.allPostingLists.add((int) currentOffset, new NewPosting(docID));
    }

    // case term already appeared but in different document
    public void addPostingOfExistingTerm(long offset, long docID, long df){ //add a new posting in existing posting list
        this.allPostingLists.add((int) (offset + df), new NewPosting(docID)); //we have offset+df to add the new posting at the end of the posting list
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
        for (NewPosting post : allPostingLists) { // Here we are looking for the number of bytes we will need for our compressed numbers
            int numOfByteNecessary = (int) (Math.floorDiv(post.getTF(), 8) + 1); // qui si può anche fare tutto con una variabile sola
            numOfBitsNecessary += (numOfByteNecessary * 8); // però diventa illeggibile quindi facciamolo alla fine
        }
        boolean[] result = new boolean[numOfBitsNecessary];
        int j = 0;
        for(NewPosting post : allPostingLists){
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

    public static void decompressionListOfTfs(byte[] compression){
        String s = compression.toString();

    }

   /* public byte[] compressionListOfDocId(){
        //Use Gamma Compression
        //sx -> Unary della dim dei bit che ci vogliono per conversione in binario
        //dx -> Concersione in binario meno cifra significativa
        for(newPosting post : allPostingLists){

        }
    }*/


    public static void main(String[] argv ){

        //a=1/3 b=2/2 c=3/4 e=4/2 f=5/1 g=6/11
        NewPosting a = new NewPosting(1);
        a.incrementDocumentFrequency();
        a.incrementDocumentFrequency();
        NewPosting b = new NewPosting(2);
        b.incrementDocumentFrequency();
        NewPosting c = new NewPosting(3);
        c.incrementDocumentFrequency();
        c.incrementDocumentFrequency();
        c.incrementDocumentFrequency();
        NewPosting e = new NewPosting(4);
        e.incrementDocumentFrequency();
        NewPosting f = new NewPosting(5);
        NewPosting g = new NewPosting(6);
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
    }
}
