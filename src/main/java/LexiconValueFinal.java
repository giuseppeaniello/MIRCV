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
        int count =0;
        for (byte b : value) {
            if(count<4)
                lexValue.setCf((lexValue.getCf() << 8) + (b & 0xFF));
            else if(count <8 && count>=4)
                lexValue.setDf((lexValue.getDf() << 8) + (b & 0xFF));
            else if(count <12 && count>=8)
                lexValue.setnBlock((lexValue.getnBlock() << 8) + (b & 0xFF));
            else if(count<20 && count >= 12)
                lexValue.setOffsetSkipBlocks((lexValue.getOffsetSkipBlocks() << 8) + (b & 0xFF));
            //else
              //  lexValue.setTermUpperBound((lexValue.getTermUpperBound() << 8) + (b & 0xFF));
            count ++;
        }
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
