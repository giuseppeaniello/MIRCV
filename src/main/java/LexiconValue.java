public class LexiconValue {

    private int  cf;
    private long df;
    private long offset;
    private int lastDocument;

    public LexiconValue (long currentOffset, int lastDocument){

        this.cf             = 1;
        this.df             = 1;
        this.offset         = currentOffset;
        this.lastDocument   = lastDocument;

    }

    public int getCf() {
        return cf;
    }

    public int getLastDocument() {
        return lastDocument;
    }

    public long getDf() {
        return df;
    }

    public long getOffset() {
        return offset;
    }

    public void setCf(int cf) {
        this.cf = cf;
    }

    public void setDf(long df) {
        this.df = df;
    }

    public void setLastDocument(int lastDocument) {
        this.lastDocument = lastDocument;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

}
