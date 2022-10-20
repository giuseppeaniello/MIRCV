import java.io.*;
import java.util.HashMap;

public class InvertedIndex {

    HashMap<String, PostingList> invertedIndex;

    public InvertedIndex(){
        this.invertedIndex = new HashMap<>();
    }

    public void addTermToIndex(String term, int docId, int position){ //codice per creare l'inverted index di un singolo blocco
        if (this.invertedIndex.containsKey(term)){
            int index = this.invertedIndex.get(term).getPosting(docId);
            if(index != -1){
                // caso esiste già posting di quel documento
                this.invertedIndex.get(term).postingList.get(index).addPosition(position);
            }
            else{
                // caso non c'è ancora posting relativo a quel documento
                Posting newPost = new Posting(docId, position);
                this.invertedIndex.get(term).addPosting(newPost);
            }
        }
        else {
            Posting firstPost = new Posting(docId, position);
            PostingList posList = new PostingList();
            posList.addPosting(firstPost);
            this.invertedIndex.put(term, posList);
        }
    }

    public void saveIndexOnDisk(String outputPath){
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputPath));){
            oos.writeObject(this.invertedIndex);
            oos.flush();
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readIndexFromDisk(String inputPath){
        try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inputPath));){
            this.invertedIndex = (HashMap<String, PostingList>) ois.readObject();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) { //metti queste eccezioni nell'ordine giusto
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void stampaLista(){
        System.out.println("Stampa lista");
        for(int i=0; i<this.invertedIndex.keySet().size(); i++){
            System.out.print("termine: " + this.invertedIndex.keySet().toArray()[i] + " " );
            this.invertedIndex.get(this.invertedIndex.keySet().toArray()[i]).stampaLista2();
            System.out.println(" ");
        }

    }

    public static void main(String[] args){
        InvertedIndex invInd = new InvertedIndex();
        String docTest = "ciao ciao de santis ciao";
        String docTest2 = "santis è il numero uno ciao";
        int docID1 = 1;
        int docID2 = 2;

        for(int i=0; i<docTest.split(" ").length; i++){
            invInd.addTermToIndex(docTest.split(" ")[i], docID1, i);
        }
        for(int i=0; i<docTest2.split(" ").length; i++){
            invInd.addTermToIndex(docTest2.split(" ")[i], docID2, i);
        }

        invInd.stampaLista();
        invInd.saveIndexOnDisk("testIndex.txt");

        System.out.println( "\n l'indice letto da file è: ");
        InvertedIndex invInd2 = new InvertedIndex();
        invInd2.readIndexFromDisk("testIndex.txt");
        invInd2.stampaLista();
    }

}
