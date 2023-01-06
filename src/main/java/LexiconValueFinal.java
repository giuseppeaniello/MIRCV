import java.nio.ByteBuffer;

public class LexiconValueFinal {

    private int cf;
    private int df;
    private int nBlock;
    private long offsetSkipBlocks;

    private float termUpperBound;

    public LexiconValueFinal(){
        this.cf = 0;
        this.df = 0;
        this.nBlock = 0;
        this.offsetSkipBlocks = 0;
        this.termUpperBound = 0;

    }
    public byte[] transformValueToByte() {
        ByteBuffer bb = ByteBuffer.allocate(24);

        bb.putInt(getCf());
        bb.putInt(getDf());
        bb.putInt(getnBlock());
        bb.putLong(getOffsetSkipBlocks());
        bb.putFloat(getTermUpperBound());

        return bb.array();
    }
    public static LexiconValueFinal transformByteToValue(byte [] value){
        LexiconValueFinal lexValue = new LexiconValueFinal();
        ByteBuffer buffer = ByteBuffer.wrap(value);
        lexValue.setCf(buffer.getInt());
        lexValue.setDf(buffer.getInt());
        lexValue.setnBlock(buffer.getInt());
        lexValue.setOffsetSkipBlocks(buffer.getLong());
        lexValue.setTermUpperBound(buffer.getFloat());
        return lexValue;
    }

    public float getTermUpperBound() {
        return termUpperBound;
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

    public void setTermUpperBound(float termUpperBound) {
        this.termUpperBound = termUpperBound;
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
