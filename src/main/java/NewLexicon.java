import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NewLexicon {


    LinkedHashMap<Text,LexiconValue> lexicon;
    int indexOfFile;
    long currentOffset;

    public NewLexicon(int indexOfFile){
        lexicon             = new LinkedHashMap<>();
        this.indexOfFile    = indexOfFile;
        this.currentOffset  = 0;
    }

    public void addElement(Text term, int docId){

        if(!lexicon.containsKey(term)){
            LexiconValue lexiconValue = new LexiconValue(currentOffset, docId);
            lexicon.put(term,lexiconValue);
            currentOffset += 1;
            lexiconValue.setLastDocument(docId);
        }
        else{
            if(lexicon.get(term).getLastDocument() == docId){
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
            }
            else{
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                lexicon.get(term).setDf(lexicon.get(term).getDf() + 1);
                lexicon.get(term).setLastDocument(docId);
                currentOffset += 1;
            }
        }
    }

    public void sortLexicon(){
        //non va bene
        List<Text> sortedKeys = new ArrayList<>();
        for(Text term : lexicon.keySet())
            sortedKeys.add(term);

        sortedKeys.sort(null);
        LinkedHashMap<Text, LexiconValue> sortedLexicon = new LinkedHashMap<>();
        for(Text term : sortedKeys){
            sortedLexicon.put(term, lexicon.get(term));
            lexicon.remove(term);
        }
        lexicon = sortedLexicon;
        sortedLexicon = null;
        sortedKeys = null;
        System.gc();
    }

    public void saveLexicon(String filePath,NewLexicon lex, int position)throws IOException {

        RandomAccessFile file = new RandomAccessFile(filePath,"rw");
        file.seek(position);
        //Convertire struttura in byte
        //file.write(convertLexToByte(lex));
        file.close();


    }

    public byte[] convertLexToByte(NewLexicon lex){

        int size = 0;
        byte[] lexiconByte = new byte[0];

        return lexiconByte;
    }

    public void updateAllOffsets(){
        boolean first = true;
        long offset = 0;
        long df = 0;

        for(Text term : lexicon.keySet()){

            if(first){
                df = lexicon.get(term).getDf();
                first = false;
            }
            else{
                lexicon.get(term).setOffset(offset + df);
                df = lexicon.get(term).getDf();
                offset = lexicon.get(term).getOffset();
            }
        }

    }




    public static void main (String[] arg){

        String a = "cIAO";
        Text aT = new Text(a);
        String b = "bau";
        Text bT = new Text(b);
        String c = "cIAO";
        Text cT = new Text(c);
        String d = "a";
        Text dT = new Text(d);
        String e = "cIAO";
        Text eT = new Text(e);

        NewLexicon newLex = new NewLexicon(0);
        newLex.addElement(aT,1);
        newLex.addElement(bT,1);
        newLex.addElement(cT,1);
        newLex.addElement(dT,1);
        newLex.addElement(eT,2);

        newLex.updateAllOffsets();
        newLex.sortLexicon();
    }
}
