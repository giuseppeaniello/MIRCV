package indexing;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class LexiconLineFinal {
    private Text term;
    private int  cf;
    private int df;
    float termUpperBoundTFIDF;
    float termUpperBoundBM25;
    private int nBlock;
    private long offsetSkipBlocks;

    public LexiconLineFinal(){
        this.term = null;
        this.cf = 0;
        this.df = 0;
        this.nBlock = 0;
        this.offsetSkipBlocks = -1;
        this.termUpperBoundTFIDF = 0;
        this.termUpperBoundBM25 = 0;
    }

    public long getOffsetSkipBlocks() {
        return offsetSkipBlocks;
    }

    public int getCf() {
        return cf;
    }

    public int getDf() {
        return df;
    }

    public int getnBlock() {
        return nBlock;
    }

    public float getTermUpperBoundBM25() {
        return termUpperBoundBM25;
    }

    public float getTermUpperBoundTFIDF() {
        return termUpperBoundTFIDF;
    }

    public Text getTerm() {
        return term;
    }

    public void setOffsetSkipBlocks(long offsetSkipBlocks) {
        this.offsetSkipBlocks = offsetSkipBlocks;
    }

    public void setCf(int cf) {
        this.cf = cf;
    }

    public void setnBlock(int nBlock) {
        this.nBlock = nBlock;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public void setTerm(Text term) {
        this.term = term;
    }

    public void setTermUpperBoundBM25(float termUpperBoundBM25) {
        this.termUpperBoundBM25 = termUpperBoundBM25;
    }

    public void setTermUpperBoundTFIDF(float termUpperBoundTFIDF) {
        this.termUpperBoundTFIDF = termUpperBoundTFIDF;
    }

    public LexiconValueFinal getLexiconValueFinal(){
        LexiconValueFinal l = new LexiconValueFinal();
        l.setCf(getCf());
        l.setDf(getCf());
        l.setOffsetSkipBlocks(getOffsetSkipBlocks());
        l.setnBlock(getnBlock());
        l.setTermUpperBoundTFIDF(getTermUpperBoundTFIDF());
        l.setTermUpperBoundBM25(getTermUpperBoundBM25());
        return l;
    }

    // method to read a LexiconLineFinal from file
    public static LexiconLineFinal readLineLexicon(FileChannel fc, long offset) throws IOException {
        ByteBuffer buffer;
        LexiconLineFinal lex = new LexiconLineFinal();
        fc.position(offset);
        buffer = ByteBuffer.allocate(22);
        do {
            fc.read(buffer);
        } while (buffer.hasRemaining());
        lex.setTerm(new Text(buffer.array()));

        buffer.clear();
        fc.position( offset+ 22);
        buffer = ByteBuffer.allocate(28);
        do {
            fc.read(buffer);
        } while (buffer.hasRemaining());
        LexiconValueFinal val = LexiconValueFinal.transformByteToValue(buffer.array());
        lex.setCf(val.getCf());
        lex.setDf(val.getDf());
        lex.setnBlock(val.getnBlock());
        lex.setOffsetSkipBlocks(val.getOffsetSkipBlocks());
        lex.setTermUpperBoundTFIDF(val.getTermUpperBoundTFIDF());
        lex.setTermUpperBoundBM25(val.getTermUpperBoundBM25());
        buffer.clear();
        return lex;
    }
}
