import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class SkipBlock {

    long finalDocId;
    int lenBlockDocId;
    int lenBlockTf;
    long offsetDocId;
    long offsetTf;

    public SkipBlock(){
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

    public void setLenBlockDocId(int lenBlockId) {
        this.lenBlockDocId = lenBlockId;
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

    public void saveSkipInfoBlock(FileChannel fc, long startingPoint, byte[] info) throws IOException {

        ByteBuffer buffer = null;
        fc.position(startingPoint);
        buffer = ByteBuffer.wrap(info);
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        buffer.clear();

    }

    public static SkipBlock readSkipBlockFromFile(FileChannel fc, long startReadingPosition) throws IOException {

        ByteBuffer buffer = null;
        SkipBlock skipInf = null;

        fc.position(startReadingPosition);
        buffer = ByteBuffer.allocate(32);
        do {
            fc.read(buffer);
        } while (buffer.hasRemaining());
        skipInf = transformSkipInfoByteToValue(buffer.array());
        buffer.clear();

        return skipInf;
    }
    public static SkipBlock readSkipBlockFromFileMap(MappedByteBuffer map, long startReadingPosition) throws IOException {

        SkipBlock skipInf = null;
        byte[] result = new byte[32];
        map.position((int) startReadingPosition);
        map.get(result,0,32);

        skipInf = transformSkipInfoByteToValue(result);

        return skipInf;
    }

    public void printSkipInfo(){
        System.out.println("LastDoc: "+getFinalDocId()+" LenDocID: "+getLenBlockDocId()+ " LenTf: "+getLenBlockTf()+
                " OffDocId: "+getoffsetDocId()+" OffTf: "+getOffsetTf());
    }

    public static SkipBlock transformSkipInfoByteToValue(byte[] value){
        SkipBlock skipInf = new SkipBlock();//Vedere che valori mettere
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
            else
                skipInf.setOffsetTf((skipInf.getOffsetTf() << 8) + (b & 0xFF));
            count ++;
        }
        return skipInf;
    }



}
