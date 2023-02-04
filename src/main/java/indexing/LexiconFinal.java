package indexing;

import org.apache.hadoop.io.Text;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeMap;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class LexiconFinal {

    public TreeMap<Text,LexiconValueFinal> lexicon;

    public LexiconFinal(){
        this.lexicon = new TreeMap<>();
    }

    public void saveLexiconFinal(FileChannel fc) throws IOException {

        ByteBuffer buffer = null;
        for(Text key : lexicon.keySet()){
            buffer = ByteBuffer.wrap(key.getBytes());
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
            byte[] valueByte = lexicon.get(key).transformValueToByte();
            buffer = ByteBuffer.wrap(valueByte);
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
        }
    }

    public void printLexiconFinal(){
        for (Text term: lexicon.keySet()){
            System.out.println("Term: "+ term +" Cf: "+lexicon.get(term).getCf()+" DF: "+lexicon.get(term).getDf()+
                    " NBlock: "+lexicon.get(term).getnBlock()+" OffsetBlock: "+lexicon.get(term).getOffsetSkipBlocks()
                        +" TermUpperBoundTFIDF: "+lexicon.get(term).getTermUpperBoundTFIDF() + " TermUpperBoundBM25: " + lexicon.get(term).getTermUpperBoundBM25());
        }
    }

}
