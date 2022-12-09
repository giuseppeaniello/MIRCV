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
    private long df;
    private long offsetDocID;
    private long offsetTF;
    private int lenOfDocID;
    private int lenOfTF;

    LexiconLine(){
        this.term = null;
        this.cf = 0;
        this.df = 0;
        this.lenOfTF = 0;
        this.lenOfDocID = 0;
        this.offsetDocID = 0;
        this.offsetTF = 0;
    }
    public int getCf() {
        return cf;
    }

    public int getLenOfDocID() {
        return lenOfDocID;
    }

    public int getLenOfTF() {
        return lenOfTF;
    }

    public long getDf() {
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

    public void setDf(long df) {
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

}
