import java.io.*;
import java.util.HashMap;
import java.util.HashSet;

public class Lexicon {

    HashMap<String, Integer> lexicon;
    HashMap<String, HashSet> documentsAlreadyPresent;

    public Lexicon(){
        this.lexicon = new HashMap<String,Integer>();
        this.documentsAlreadyPresent = new HashMap<String, HashSet>();
    }

    public void checkNewTerm(String term, String docID){
        if(lexicon.containsKey(term)){    // se la chiave c'è già non aggiungiamo il termine e valutiamo se era già comparso il termine in questo document
            if(documentsAlreadyPresent.get(term).contains(docID)) // se era già comparso non facciamo nulla
                return;
            else{
                lexicon.replace(term, lexicon.get(term),lexicon.get(term)+1); // se è la prima volta che compare in questo document aggiorniamo la DF
                documentsAlreadyPresent.get(term).add(docID); // e aggiungiamo il document al set di document già comparsi
            }
        }
        else{
            lexicon.put(term, 1);
            HashSet emptyHashSet = new HashSet();
            emptyHashSet.add(docID);
            documentsAlreadyPresent.put(term, emptyHashSet);
        }
    }


    public void saveLexiconInDisk(String outputPath){ // salvataggio sbagliato, manca la compressione
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputPath));){
            oos.writeObject(this.lexicon);
            oos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readLexiconFromDisk(String inputPath){ // lettura sbagliata, manca la decompressione
        try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inputPath))){
            this.lexicon = (HashMap<String, Integer>) ois.readObject();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) { //metti queste eccezioni nell'ordine giusto
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // main for testing Lexicon class
    public static void main (String[] args){
        String pippo = "pippo";
        String pluto = "pluto";
        String paperino = "paperino";
        String pippo2 = "pippo";
        String pippo3 = "pippo";

        Lexicon lex = new Lexicon();
        lex.checkNewTerm(pippo, "1");
        lex.checkNewTerm(pluto, "2");
        lex.checkNewTerm(paperino,"1");
        lex.checkNewTerm(pippo2,"1");
        lex.checkNewTerm(pippo3, "2");

        lex.saveLexiconInDisk("TestDictionary.txt");

        Lexicon lex2 = new Lexicon();
        lex2.readLexiconFromDisk("TestDictionary.txt");
        for (String s: lex2.lexicon.keySet()) {
            System.out.print(s + " ");
            System.out.println(lex2.lexicon.get(s));
        }
    }

}
