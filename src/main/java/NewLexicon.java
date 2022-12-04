import org.apache.hadoop.hdfs.util.ByteArray;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.READ;
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
            updateAllOffsets();
        }
        else{
            if(lexicon.get(term).getLastDocument() == docId){
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                updateAllOffsets();
            }
            else{
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                lexicon.get(term).setDf(lexicon.get(term).getDf() + 1);
                lexicon.get(term).setLastDocument(docId);
                currentOffset += 1;
                updateAllOffsets();
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
                byte[] valueByte = transformValueToByte(lexicon.get(key).getCf(), lexicon.get(key).getDf(), lexicon.get(key).getOffset());
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




    public static void main (String[] arg) throws IOException {
        NewLexicon lex = new NewLexicon(0);
        lex.addElement(new Text("pippo               "), 1);
        lex.addElement(new Text("pippo2              "), 1);
        lex.addElement(new Text("pippo3              "), 1);
        lex.saveLexicon("prova", 0);

    }
}
