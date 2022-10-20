import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
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


    public void saveLexiconInDisk(String outputPathListTerms, String outputPathListDocumentFrequencies){ // manca la compressione
        try(ObjectOutputStream ooslt = new ObjectOutputStream(new FileOutputStream(outputPathListTerms));
            ObjectOutputStream oosldf = new ObjectOutputStream(new FileOutputStream(outputPathListDocumentFrequencies))){

            ArrayList<String> tmp1 = new ArrayList<>();
            tmp1.addAll(this.lexicon.keySet());
            ooslt.writeObject(tmp1);
            ooslt.flush();

            ArrayList<Integer> tmp2 = new ArrayList<>();
            tmp2.addAll(this.lexicon.values());
            oosldf.writeObject(tmp2);
            oosldf.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readLexiconFromDisk(String inputPathListTerms, String inputPathListDocumentFrequencies){ // manca la decompressione
        try(ObjectInputStream oislt = new ObjectInputStream(new FileInputStream(inputPathListTerms));
            ObjectInputStream oisldf = new ObjectInputStream(new FileInputStream(inputPathListDocumentFrequencies))){
            this.lexicon = createHashMapFromLists((ArrayList<String>) oislt.readObject(), (ArrayList<Integer>) oisldf.readObject());
           // this.lexicon = (HashMap<String, Integer>) ois.readObject();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) { //metti queste eccezioni nell'ordine giusto
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private HashMap createHashMapFromLists(ArrayList<String> listKeys, ArrayList<Integer> listValues){
        HashMap result = new HashMap();
        for(int i=0; i<listKeys.size(); i++)
            result.put(listKeys.get(i), listValues.get(i));
        return result;
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

        lex.saveLexiconInDisk("TestDictionaryKeys.txt", "TestDictionaryValues.txt");

        Lexicon lex2 = new Lexicon();
        lex2.readLexiconFromDisk("TestDictionaryKeys.txt", "TestDictionaryValues.txt");
        for (String s: lex2.lexicon.keySet()) {
            System.out.print(s + " ");
            System.out.println(lex2.lexicon.get(s));
        }
    }

}
