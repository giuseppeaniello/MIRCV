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
    TreeMap<Text,LexiconValueFinal> lexicon;

    public LexiconFinal(){
        this.lexicon = new TreeMap<>();
    }

    public void saveLexiconFinal(String filePath) throws FileNotFoundException {
        RandomAccessFile file = new RandomAccessFile(filePath ,"rw");
        Path fileP = Paths.get(filePath );
        ByteBuffer buffer = null;
        try (FileChannel fc = FileChannel.open(fileP, WRITE)) {
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
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
    }

    public static LexiconFinal readFinalLexiconFromFile(String filePath){
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer;
        LexiconFinal lex = new LexiconFinal();

        try (FileChannel fc = FileChannel.open(fileP, READ))
        {

            for(int i = 0; i<fc.size(); i=i+46) {
                fc.position(i);
                buffer = ByteBuffer.allocate(20); //50 is the total number of bytes to read a complete term of the lexicon
                do {
                    fc.read(buffer);
                } while (buffer.hasRemaining());
                Text term = new Text(buffer.array());
                buffer.clear();

                fc.position( i+ 22);
                buffer = ByteBuffer.allocate(24); //42 is the total number of bytes to read a complete term of the lexicon
                do {
                    fc.read(buffer);
                } while (buffer.hasRemaining());
                LexiconValueFinal value = LexiconValueFinal.transformByteToValue(buffer.array());
                buffer.clear();

                lex.lexicon.put(term,value);
            }

        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return lex;
    }
}
