public class LexiconValue {

    private int  cf;
    private long df;
    private long offsetDocID;
    private long offsetTF;
    private int lastDocument;

    public LexiconValue (long currentOffset, int lastDocument){

        this.cf             = 1;
        this.df             = 1;
        this.offsetDocID = currentOffset;
        this.lastDocument   = lastDocument;
        this.offsetTF = 0;

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

    public long getOffsetDocID() {
        return offsetDocID;
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

    public void setOffsetDocID(long offsetDocID) {
        this.offsetDocID = offsetDocID;
    }

    public long getOffsetTF(){
        return offsetTF;
    }

    public void setOffsetTF(long offsetTF){
        this.offsetTF = offsetTF;
    }
}
