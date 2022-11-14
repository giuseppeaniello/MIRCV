import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import java.util.HashSet;


public class Dictionary {
    DB db;
    DB db2;
    HTreeMap<String, Integer> dictionary;
    HTreeMap<String, HashSet> documentsAlreadyPresent;

    public Dictionary(int indexOfBlock){
        db = DBMaker.fileDB("dictionaryDB" + indexOfBlock + ".db" ).make();
        dictionary = db.hashMap("dictionaryOnFile" + indexOfBlock).keySerializer(Serializer.STRING).valueSerializer(Serializer.INTEGER).create();
        db2 = DBMaker.fileDB("fileSetDB" + indexOfBlock + ".db" ).make();
        documentsAlreadyPresent = db2.hashMap("setOnFile" + indexOfBlock).keySerializer(Serializer.STRING).valueSerializer(Serializer.JAVA).create();
    }

    public void addTermToDictionary(String term, String docID){
        if(dictionary.containsKey(term)){    // se la chiave c'è già non aggiungiamo il termine e valutiamo se era già comparso il termine in questo document
            if(documentsAlreadyPresent.get(term).contains(docID)) // se era già comparso non facciamo nulla
                return;
            else{
                dictionary.replace(term, dictionary.get(term),dictionary.get(term)+1); // se è la prima volta che compare in questo document aggiorniamo la DF
                documentsAlreadyPresent.get(term).add(docID); // e aggiungiamo il document al set di document già comparsi
            }
        }
        else{
            dictionary.put(term, 1);
            HashSet emptyHashSet = new HashSet();
            emptyHashSet.add(docID);
            documentsAlreadyPresent.put(term, emptyHashSet);
        }
    }


    public void closeDictionary(){
        db.close();
        db2.close();
        dictionary.close();
        documentsAlreadyPresent.close();
    }

    public static void main (String[] args){
        String pippo = "pippo";
        String pluto = "aaaaaaato";
        String paperino = "paperino";
        String pippo2 = "pippo";
        String pippo3 = "pippo";

        Dictionary lex = new Dictionary(6);
        lex.addTermToDictionary(pippo, "1");
        lex.addTermToDictionary(pluto, "2");
        lex.addTermToDictionary(paperino,"1");
        lex.addTermToDictionary(pippo2,"1");
        lex.addTermToDictionary(pippo3, "2");

        for (String s: lex.dictionary.keySet()) {
            System.out.print(s + " ");
            System.out.println(lex.dictionary.get(s));
        }
        lex.closeDictionary();
    }




}
