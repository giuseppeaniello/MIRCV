package indexing;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import static java.nio.file.StandardOpenOption.READ;

public class LexiconLine {

    private Text term;
    private int  cf;
    private int df;
    private long offsetDocID;
    private long offsetTF;
    private int lenOfDocID;
    private int lenOfTF;
    private int nBlock;
    private long offsetSkipBlocks;

    LexiconLine(){
        this.term = null;
        this.cf = 0;
        this.df = 0;
        this.lenOfTF = 0;
        this.lenOfDocID = 0;
        this.offsetDocID = 0;
        this.offsetTF = 0;
        this.offsetSkipBlocks = 0;
        this.nBlock = 0;
    }

    public long getOffsetSkipBlocks() {
        return offsetSkipBlocks;
    }

    public int getnBlock() {
        return nBlock;
    }

    public void setOffsetSkipBlocks(long offsetSkipBlocks) {
        this.offsetSkipBlocks = offsetSkipBlocks;
    }

    public void setnBlock(int nBlock) {
        this.nBlock = nBlock;
    }

    public int getCf() {
        return cf;
    }

    public int getLenOffDocID() {
        return lenOfDocID;
    }

    public int getLenOffTF() {
        return lenOfTF;
    }

    public int getDf() {
        return df;
    }

    public long getOffsetDocID() {
        return offsetDocID;
    }

    public long getOffsetTF() {
        return offsetTF;
    }

    public Text getTerm() {
        return term;
    }

    public void setCf(int cf) {
        this.cf = cf;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public void setLenOfDocID(int lenOfDocID) {
        this.lenOfDocID = lenOfDocID;
    }

    public void setLenOfTF(int lenOfTF) {
        this.lenOfTF = lenOfTF;
    }

    public void setOffsetDocID(long offsetDocID) {
        this.offsetDocID = offsetDocID;
    }

    public void setOffsetTF(long offsetTF) {
        this.offsetTF = offsetTF;
    }

    public void setTerm(Text term) {
        this.term = term;
    }

    public void printLexiconLine(){
        System.out.println(this.term +" "+this.cf+" "+this.df+" "+this.offsetDocID+" "+
                this.offsetTF+ " "+ this.lenOfDocID+" "+this.lenOfTF);
    }

    // method to save a LexiconLine on file
    public void saveLexiconLineOnFile(FileChannel fc,long offset) throws IOException {
        ByteBuffer buffer = null;
        fc.position(offset);
        buffer = ByteBuffer.wrap(getTerm().getBytes());
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        buffer.clear();
        byte[] valueByte =Lexicon.transformValueToByte(getCf(), getDf(), getOffsetDocID(), getOffsetTF(),
                getLenOffDocID(), getLenOffTF()) ;
        fc.position(offset+22);
        buffer = ByteBuffer.wrap(valueByte);
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        buffer.clear();
    }


    public byte[] transformValueToByteWithSkip() {
        ByteBuffer bb = ByteBuffer.allocate(20);
        bb.putInt(getCf());
        bb.putInt(getDf());
        bb.putLong(getOffsetSkipBlocks());
        bb.putInt(getnBlock());

        return bb.array();
    }

    // method to read bytes of a LexiconLine on file and convert it into an actual LexiconLine object
    public static LexiconLine transformByteWIthSkipToLexicon(byte[] value){
        LexiconLine l = new LexiconLine();
        int count =0;
        for (byte b : value) {
            if(count<4)
                l.setCf((l.getCf() << 8) + (b & 0xFF));
            else if(count <8 && count>=4)
                l.setDf((l.getDf() << 8) + (b & 0xFF));
            else if(count <16 && count>=8)
                l.setOffsetSkipBlocks((l.getOffsetSkipBlocks() << 8) + (b & 0xFF));
            else
                l.setnBlock((l.getnBlock() << 8) + (b & 0xFF));
            count ++;
        }
        return l;
    }

    // Method to save a single LexiconLine with also skip info
    public void saveLexiconLineWithSkip(FileChannel fc, long startingPoint) throws IOException {
        ByteBuffer buffer = null;
        fc.position(startingPoint);
        buffer = ByteBuffer.wrap(getTerm().getBytes());
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        buffer.clear();
        fc.position(startingPoint+22);
        byte[] valueByte =transformValueToByteWithSkip() ;
        buffer = ByteBuffer.wrap(valueByte);
        while (buffer.hasRemaining()) {
            fc.write(buffer);
        }
        buffer.clear();
    }

    public void printLexiconLineWithSkip(){
        System.out.println("TERM: "+getTerm()+"CF: "+ getCf()+" DF: "+getDf()+" Off: "+getOffsetSkipBlocks()+" Nblock: "+getnBlock());
    }


}
