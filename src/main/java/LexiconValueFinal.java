import java.nio.ByteBuffer;

public class LexiconValueFinal {

    private int cf;
    private int df;
    private int nBlock;
    private long offsetSkipBlocks;
    private float termUpperBoundTFIDF;
    private float termUpperBoundBM25;

    public LexiconValueFinal(){
        this.cf = 0;
        this.df = 0;
        this.nBlock = 0;
        this.offsetSkipBlocks = 0;
        this.termUpperBoundTFIDF = 0;
        this.termUpperBoundBM25 = 0;

    }
    public byte[] transformValueToByte() {
        ByteBuffer bb = ByteBuffer.allocate(28);

        bb.putInt(getCf());
        bb.putInt(getDf());
        bb.putInt(getnBlock());
        bb.putLong(getOffsetSkipBlocks());
        bb.putFloat(getTermUpperBoundTFIDF());
        bb.putFloat(getTermUpperBoundBM25());

        return bb.array();
    }
    public static LexiconValueFinal transformByteToValue(byte[] value){
        LexiconValueFinal lexValue = new LexiconValueFinal();
        ByteBuffer bb = ByteBuffer.wrap(value);
        lexValue.setCf(bb.getInt());
        lexValue.setDf(bb.getInt());
        lexValue.setnBlock(bb.getInt());
        lexValue.setOffsetSkipBlocks(bb.getLong());
        lexValue.setTermUpperBoundTFIDF(bb.getFloat());
        lexValue.setTermUpperBoundBM25(bb.getFloat());
        return lexValue;

    }

    public float getTermUpperBoundTFIDF() {
        return termUpperBoundTFIDF;
    }

    public float getTermUpperBoundBM25() {
        return termUpperBoundBM25;
    }

    public long getOffsetSkipBlocks() {
        return offsetSkipBlocks;
    }

    public int getnBlock() {
        return nBlock;
    }

    public int getCf() {
        return cf;
    }

    public int getDf() {
        return df;
    }

    public void setTermUpperBoundTFIDF(float termUpperBoundTFIDF) {
        this.termUpperBoundTFIDF = termUpperBoundTFIDF;
    }

    public void setTermUpperBoundBM25(float termUpperBoundBM25) {
        this.termUpperBoundBM25 = termUpperBoundBM25;
    }

    public void setOffsetSkipBlocks(long offsetSkipBlocks) {
        this.offsetSkipBlocks = offsetSkipBlocks;
    }

    public void setnBlock(int nBlock) {
        this.nBlock = nBlock;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public void setCf(int cf) {
        this.cf = cf;
    }

}
