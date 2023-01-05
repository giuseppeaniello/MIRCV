import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class DocumentTable {
    public HashMap<Long, Integer> docTab; // key=docID val=length

    public DocumentTable(){
        docTab = new HashMap<>();
    }

    public void saveDocumentTable(String filePath) throws FileNotFoundException {
        RandomAccessFile file = new RandomAccessFile(filePath ,"rw");
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        try (FileChannel fc = FileChannel.open(fileP, WRITE)) {
            for(Long key : docTab.keySet()){
                buffer = ByteBuffer.wrap(convertLongToByteArr(key));
                while (buffer.hasRemaining()) {
                    fc.write(buffer);
                }
                buffer.clear();
                byte[] valueByte = transformValueDocTabToByte(docTab.get(key));
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

    private static byte[] transformValueDocTabToByte(int length) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(length);
        return bb.array();
    }



    public byte[] convertLongToByteArr(long number){
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(number);
        return bb.array();
    }


}
