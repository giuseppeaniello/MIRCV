import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class SkipInfo {
    long finalDocId;
    int lenBlockDocId;
    int lenBlockTf;
    long offsetDocId;
    long offsetTf;

    public SkipInfo(){
        this.finalDocId = 0;
        this.lenBlockDocId = 0;
        this.offsetDocId = 0;
        this.offsetTf = 0;
        this.lenBlockTf = 0;
    }

    public int getLenBlockTf() {
        return lenBlockTf;
    }

    public void setLenBlockTf(int lenBlockTf) {
        this.lenBlockTf = lenBlockTf;
    }

    public long getOffsetTf() {
        return offsetTf;
    }

    public void setOffsetTf(long offsetTf) {
        this.offsetTf = offsetTf;
    }

    public long getoffsetDocId() {
        return offsetDocId;
    }

    public void setOffsetDocId(long offsetDocId) {
        this.offsetDocId = offsetDocId;
    }

    public int getLenBlockDocId() {
        return lenBlockDocId;
    }

    public long getFinalDocId() {
        return finalDocId;
    }

    public void setFinalDocId(long finalDocId) {
        this.finalDocId = finalDocId;
    }

    public void setLenBlockDocId(int lenBlock) {
        this.lenBlockDocId = lenBlock;
    }
    public byte[] trasformInfoToByte(){
        byte[] converted;
        ByteBuffer bb = ByteBuffer.allocate(32);
        bb.putLong(this.finalDocId);
        bb.putInt(this.lenBlockDocId);
        bb.putInt(this.lenBlockTf);
        bb.putLong(this.offsetDocId);
        bb.putLong(this.offsetTf);
        converted=bb.array();
        return converted;
    }

    public  void saveSkipInfoBlock(String path, long startingPoint, byte[] info) throws FileNotFoundException {
        RandomAccessFile fileTf = new RandomAccessFile(path ,"rw");
        Path fileP = Paths.get(path);
        ByteBuffer buffer = null;

        try (FileChannel fc = FileChannel.open(fileP, WRITE)) {
            fc.position(startingPoint);
            buffer = ByteBuffer.wrap(info);
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
    }

    public static SkipInfo readSkipInfoFromFile(String filePath, long startReadingPosition){
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        SkipInfo skipInf = null;
        try (FileChannel fc = FileChannel.open(fileP, READ))
        {
            fc.position(startReadingPosition);
            buffer = ByteBuffer.allocate(32);
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            skipInf = transformSkipInfoByteToValue(buffer.array());
            buffer.clear();
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return skipInf;
    }

    public static SkipInfo transformSkipInfoByteToValue(byte[] value){
        SkipInfo skipInf = new SkipInfo();//Vedere che valori mettere
        int count = 0;
        for (byte b : value) {
            if(count<8)
                skipInf.setFinalDocId((skipInf.getFinalDocId() << 8) + (b & 0xFF));
            else if(count <12 && count>=8)
                skipInf.setLenBlockDocId((skipInf.getLenBlockDocId() << 8) + (b & 0xFF));
            else if(count <16 && count>=12)
                skipInf.setLenBlockTf((skipInf.getLenBlockTf() << 8) + (b & 0xFF));
            else if(count<24 && count >= 16)
                skipInf.setOffsetDocId((skipInf.getoffsetDocId() << 8) + (b & 0xFF));
            else if(count<32 && count >= 24)
                skipInf.setOffsetTf((skipInf.getOffsetTf() << 8) + (b & 0xFF));
            count ++;
        }
        return skipInf;
    }

}
