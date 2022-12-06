public class LexiconValue {

    private int  cf;
    private long df;
    private long offsetInList;
    private long offsetDocID;
    private long offsetTF;
    private long lastDocument;
    private int lenOfDocID;
    private int lenOfTF;

    public LexiconValue (long currentOffset, long lastDocument){

        this.cf = 1;
        this.df = 1;
        this.offsetInList = currentOffset;
        this.lastDocument = lastDocument;
        this.offsetTF = 0;
        this.lenOfTF = 0;
        this.lenOfDocID = 0;

    }

    public int getCf() {
        return cf;
    }

    public long getLastDocument() {
        return lastDocument;
    }

    public long getDf() {
        return df;
    }

    public long getOffsetInList() {
        return offsetInList;
    }

    public void setCf(int cf) {
        this.cf = cf;
    }

    public void setDf(long df) {
        this.df = df;
    }

    public void setLastDocument(long lastDocument) {
        this.lastDocument = lastDocument;
    }

    public void setOffsetInList(long offsetInList) {
        this.offsetInList = offsetInList;
    }

    public long getOffsetTF(){
        return offsetTF;
    }

    public void setOffsetDocID(long offsetDocID) {
        this.offsetDocID = offsetDocID;
    }

    public long getOffsetDocID(){ return offsetDocID; }

    public void setOffsetTF(long offsetTF){
        this.offsetTF = offsetTF;
    }

    public void setLenOfDocID(int lenOfDocID){
        this.lenOfDocID = lenOfDocID;
    }

    public void setLenOfTF(int lenOfTF){
        this.lenOfTF = lenOfTF;
    }

    public int getLenOfDocID(){
        return this.lenOfDocID;
    }

    public int getLenOfTF(){
        return this.lenOfTF;
    }
}
