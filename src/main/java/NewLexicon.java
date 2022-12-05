import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static java.nio.file.StandardOpenOption.WRITE;

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
            updateAllOffsetsDocID();
        }
        else{
            if(lexicon.get(term).getLastDocument() == docId){
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                updateAllOffsetsDocID();
            }
            else{
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                lexicon.get(term).setDf(lexicon.get(term).getDf() + 1);
                lexicon.get(term).setLastDocument(docId);
                currentOffset += 1;
                updateAllOffsetsDocID();
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

    public void saveLexicon(String filePath, int indexOfFile)  {
        Path file = Paths.get(filePath);
        ByteBuffer buffer = null;

        try (FileChannel fc = FileChannel.open(file, WRITE)) {


            // buffer = ByteBuffer.wrap(songName.getBytes());
            for(Text key : lexicon.keySet()){
                buffer = ByteBuffer.wrap(key.getBytes());
                while (buffer.hasRemaining()) {
                    fc.write(buffer);
                }
                buffer.clear();
                byte[] valueByte = transformValueToByte(lexicon.get(key).getCf(), lexicon.get(key).getDf(), lexicon.get(key).getOffsetDocID());
                buffer = ByteBuffer.wrap(valueByte);
                while (buffer.hasRemaining()) {
                    fc.write(buffer);
                }
                buffer.clear();
            }


            fc.close();
        } catch (IOException ex) {
        System.err.println("I/O Error: " + ex);
        }
    }

    private byte[] transformValueToByte( int cF, long dF, long offset ) {
        ByteBuffer bb = ByteBuffer.allocate(20);
        bb.putInt(cF);
        bb.putLong(dF);
        bb.putLong(offset);
        return bb.array();
    }

    public void updateAllOffsetsDocID(){ // questo non va chiamato, è già nel metodo che aggiunge gli elementi
        boolean first = true;  // non lo faccio private perchè magari dopo il merging ci serve anche chiamarlo a parte
        long offset = 0;
        long df = 0;
        for(Text term : lexicon.keySet()){
            if(first){
                df = lexicon.get(term).getDf();
                first = false;
            }
            else{
                lexicon.get(term).setOffsetDocID(offset + df);
                df = lexicon.get(term).getDf();
                offset = lexicon.get(term).getOffsetDocID();
            }
        }
    }

    public void updateAllOffsetsTF(NewInvertedIndex invInd){ // to be done just one time before the sorting before the saving on file
        boolean first = true;
        long numOfBytes = 0;
        long df = 0; // df of the previous term
        for(Text term : lexicon.keySet()){
            if(first){ //the first time the offsetTF is already 0
                df = lexicon.get(term).getDf();
                for(int i=0; i<df; i++){ // df indicates the number of posting, for each posting we will need a certain number of bytes
                    numOfBytes += (long) Math.floor(invInd.allPostingLists.get((int)lexicon.get(term).getOffsetDocID() + i).getTF() / 8)+1;
                }
                first = false;
            }
            else{
                lexicon.get(term).setOffsetTF(numOfBytes);
                df = lexicon.get(term).getDf();
                for(int i=0; i<df; i++){
                    numOfBytes += (long) Math.floor(invInd.allPostingLists.get((int)lexicon.get(term).getOffsetDocID() + i).getTF() / 8)+1;
                }
            }
        }
    }




    public static void main (String[] arg) throws IOException {
        NewLexicon lex = new NewLexicon(0);
        lex.addElement(new Text("pippo               "), 1);
        lex.addElement(new Text("pippo2              "), 1);
        lex.addElement(new Text("pippo3              "), 1);
        lex.saveLexicon("prova", 0);

    }
}
