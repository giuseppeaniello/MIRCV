public class LexiconValue {

    private int  cf;
    private int df;
    private long offsetDocID;
    private long offsetTF;
    private long lastDocument;
    private int lenOfDocID;
    private int lenOfTF;
    private int index;

    public LexiconValue (long lastDocument, int index){

        this.cf = 1;
        this.df = 1;
        this.lastDocument = lastDocument;
        this.offsetTF = 0;
        this.lenOfTF = 0;
        this.lenOfDocID = 0;
        this.index = index;

    }

    public int getCf() {
        return cf;
    }

    public long getLastDocument() {
        return lastDocument;
    }

    public int getDf() {
        return df;
    }


    public void setCf(int cf) {
        this.cf = cf;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public void setLastDocument(long lastDocument) {
        this.lastDocument = lastDocument;
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

    public int getIndex(){
        return this.index;
    }

}
