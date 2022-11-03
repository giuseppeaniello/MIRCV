import java.io.*;
import java.util.ArrayList;
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


    public void saveLexiconInDisk(String outputPathListTerms){
        try {
            File file = new File(outputPathListTerms);
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);

            ArrayList<String> tmp1 = new ArrayList<>();
            tmp1.addAll(this.lexicon.keySet());
            ArrayList<Integer> tmp2 = new ArrayList<>();
            tmp2.addAll(this.lexicon.values());

            for(int i=0; i<tmp1.size(); i++) {
                bw.write(tmp1.get(i) + " " + tmp2.get(i) + "\n");
                bw.flush();
            }
            bw.close();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }


    public void readLexiconFromDisk(String inputPathListTerms){
        try {
            File file = new File(inputPathListTerms);
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();
            HashMap newMap = new HashMap();
            while(line != null) {
                newMap.put(line.split(" ")[0], line.split(" ")[1]);
                line = br.readLine();
            }
            this.lexicon = newMap;
            br.close();
        } catch(IOException e) {
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

        lex.saveLexiconInDisk("TestNuovaScrittura.txt");
        Lexicon lex2 = new Lexicon();
        lex2.readLexiconFromDisk("TestNuovaScrittura.txt");
        for (String s: lex2.lexicon.keySet()) {
            System.out.print(s + " ");
            System.out.println(lex2.lexicon.get(s));
        }
    }

}
