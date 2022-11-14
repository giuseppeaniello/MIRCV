import org.mapdb.*;
import org.mapdb.volume.MappedFileVol;
import org.mapdb.volume.Volume;

public class OnFileInvertedIndex {

    HTreeMap<String, PostingList> invertedIndex;
    DB db;

    public OnFileInvertedIndex(int indexOfBlock){
        db = DBMaker.fileDB("invIndDB" + indexOfBlock + ".db" ).make();
        invertedIndex = db.hashMap("invertedIndexOnFile" + indexOfBlock).keySerializer(Serializer.STRING).valueSerializer(Serializer.JAVA).create();
    }

    public void addTermToIndex(String term, String docId, int position){ //codice per creare l'inverted index di un singolo blocco
        if (this.invertedIndex.containsKey(term)){
            int index = this.invertedIndex.get(term).getIndexOfPosting(Integer.parseInt(docId));
            if(index != -1){
                // caso esiste già posting di quel documento
                //prendo la posting list
                PostingList newPostingList = this.invertedIndex.get(term);
                //prendo il posting da modificare
                int indexOfNewPosting = newPostingList.getIndexOfPosting(Integer.parseInt(docId));
                Posting newPosting = newPostingList.postingList.get(indexOfNewPosting);
                //aggiungo la nuova posizione nel posting
                newPosting.addPosition(position);
                //rimuovo il posting vecchio dalla posting list
                newPostingList.postingList.remove(indexOfNewPosting);
                //aggiungo il posting nuovo modificato con la posizione aggiuntiva
                newPostingList.postingList.add(newPosting);
                //rimuovo la posting list vecchia dall'inverted index
                invertedIndex.remove(term);
                //inserisco la posting list nuova
                invertedIndex.put(term,newPostingList);
            }
            else{
                // caso non c'è ancora posting relativo a quel documento ma c'è già posting per quel term
                //creo il nuovo posting
                Posting newPost = new Posting(Integer.parseInt(docId), position);
                //prendo la posting list
                PostingList newPostingList = this.invertedIndex.get(term);
                //aggiungo il posting alla nuova postinglist
                newPostingList.postingList.add(newPost);
                //elimino la vecchia posting list dall'invertedindex
                this.invertedIndex.remove(term);
                //inserisco la nuova posting list nell'inverted index
                this.invertedIndex.put(term, newPostingList);
            }
        }
        else {
            Posting firstPost = new Posting(Integer.parseInt(docId), position);
            PostingList posList = new PostingList();
            posList.addPosting(firstPost);
            this.invertedIndex.put(term, posList);
        }
    }

    public void closeInvertedIndex(){
        db.close();
        invertedIndex.close();
    }


    public static void main(String[] args){
        long freeMemory = Runtime.getRuntime().freeMemory();
        long totalMemory = Runtime.getRuntime().totalMemory();
        System.out.println("MEMORIA TOTALE INIZIO  " + totalMemory);
        System.out.println("MEMORIA LIBERA INIZIO  " + freeMemory);
        OnFileInvertedIndex invInd = new OnFileInvertedIndex(8);
        String docTest = "ciao ciao de santis ciao";
        String docTest2 = "santis è il numero uno ciao";
        String docID1 = "1";
        String docID2 = "2";

        for(int i=0; i<docTest.split(" ").length; i++){
            invInd.addTermToIndex(docTest.split(" ")[i], docID1, i);
        }
        for(int i=0; i<docTest2.split(" ").length; i++){
            invInd.addTermToIndex(docTest2.split(" ")[i], docID2, i);
        }

        PostingList ps = new PostingList();

        for(Object key:invInd.invertedIndex.keySet().toArray()) {
            ps = invInd.invertedIndex.get(key);
            System.out.print("TERM: " + key + " ");
            for(Posting posting: ps.postingList) {
                System.out.print("DOC_ID:  " + posting.docID + "   ");
                System.out.print("POSITIONS: ");
                for(int position : posting.positions) {
                    System.out.print(position + " ");
                }
            }
            System.out.println("");
        }

        invInd.closeInvertedIndex();
        freeMemory = Runtime.getRuntime().freeMemory();
        System.out.println("MEMORIA LIBERA FINE  " + freeMemory);

    }


}
