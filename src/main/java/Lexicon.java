import java.io.*;
import java.util.HashMap;

public class Lexicon {

    HashMap<String, Integer> lexicon;

    public Lexicon(){
        this.lexicon = new HashMap<String,Integer>();
    }

    public void checkNewTerm(String term){
        if(lexicon.containsKey(term))
            return;
        else
            lexicon.put(term, 1);
    }

    public void generateDocumentFrequency(){
        /* TODO:
            set the values (document frequency) for each key (term)
            of this.lexicon exploiting the length of the posting list
            of each term
         */
    }

    public void saveLexiconInDisk(String outputPath){
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputPath));){
            oos.writeObject(this.lexicon);
            oos.flush();
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readLexiconFromDisk(String inputPath){
        try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inputPath));){
            this.lexicon = (HashMap<String, Integer>) ois.readObject();
            ois.close();
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
        String a = "pippo";
        String b = "pluto";
        String c = "paperino";
        String d = "Gennaro";
        String e = "pippo";

        Lexicon lex = new Lexicon();
        lex.checkNewTerm(a);
        lex.checkNewTerm(b);
        lex.checkNewTerm(c);
        lex.checkNewTerm(d);
        lex.checkNewTerm(e);

        lex.saveLexiconInDisk("TestDictionary.txt");

        Lexicon lex2 = new Lexicon();
        lex2.readLexiconFromDisk("TestDictionary.txt");
        for (String s: lex2.lexicon.keySet())
            System.out.println(s);
    }

}
