import sun.security.util.BitArray;

import java.util.ArrayList;
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
        
    }

    public void clearInvertedIndex(){
        allPostingLists.clear();
        System.gc();
    }

    public byte[] compressListOfDocIDs(){

    }


    public byte[] compressListOfTFs() {
        int sumOfTFs = 0;
        for (newPosting post : allPostingLists) {
            sumOfTFs += post.getTF();
        }
        BitSet result = new BitSet(sumOfTFs);
        int j=0;
        for(newPosting post : allPostingLists){
            for(int i=0; i<post.getTF()-1; i++){
                result.set(j);
                j++;
            }
            j++;
        }
        return result.toByteArray();
    }

}
