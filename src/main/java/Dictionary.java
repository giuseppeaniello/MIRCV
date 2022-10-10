import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Dictionary {
    List<String> dictionary;

    public Dictionary(){   // constructor
        this.dictionary = new ArrayList<>();
    }

    // function that checks if a term is already in the dictionary
    // if it isn't it's added to it
    public void newTerm(String term){
        if (this.containsTerm(term)){  // case term already present
            return;
        }
        else{ // case term not present
            this.dictionary.add(term);
        }
    }

    // function that checks if a term is already in the dictionary
    private boolean containsTerm(String term){
        for (String tmp:this.dictionary){
            if(tmp.compareTo(term) == 0)
                return true;
        }
        return false;
    }

    // function to save the dictionary in disk
    public void saveDictionaryInDisk(String outputPath){
        try(ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(outputPath));){
            oos.writeObject(this.dictionary);
            oos.flush();
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // function to load the dictionary from the disk
    public void readDictionary(String inputPath){
        try(ObjectInputStream ois = new ObjectInputStream(new FileInputStream(inputPath));){
            this.dictionary = (ArrayList<String>) ois.readObject();
            ois.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // main for testing Dictionary class
    public static void main (String[] args){
        String a = "pippo";
        String b = "pluto";
        String c = "paperino";
        String d = "Gennaro";
        String e = "pippo";

        Dictionary dic = new Dictionary();
        dic.newTerm(a);
        dic.newTerm(b);
        dic.newTerm(c);
        dic.newTerm(d);
        dic.newTerm(e);

        dic.saveDictionaryInDisk("TestDictionary.txt");

        Dictionary dic2 = new Dictionary();
        dic2.readDictionary("TestDictionary.txt");
        for (String s: dic2.dictionary)
            System.out.println(s);
    }
}
