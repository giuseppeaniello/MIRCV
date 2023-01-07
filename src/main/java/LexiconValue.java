public class LexiconValue {

    private int  cf;
    private int df;
    private long offsetDocID;
    private long offsetTF;
    private long lastDocument;
    private int lenOfDocID;
    private int lenOfTF;
    private int index;
    private int nBlock;
    private long offsetSkipBlocks;
    private float termUpperBound;

    public LexiconValue (long lastDocument, int index){
        this.cf = 1;
        this.df = 1;
        this.lastDocument = lastDocument;
        this.offsetTF = 0;
        this.lenOfTF = 0;
        this.lenOfDocID = 0;
        this.index = index;
        this.nBlock = 0;
        this.offsetSkipBlocks=0;
        this.termUpperBound = 0;
    }

    public LexiconValue(){
        this.cf = 1;
        this.df = 1;
        this.lastDocument = 0;
        this.offsetTF = 0;
        this.lenOfTF = 0;
        this.lenOfDocID = 0;
        this.index = 0;
        this.nBlock = 0;
        this.offsetSkipBlocks=0;
        this.termUpperBound = 0;
    }

    public float getTermUpperBound() {
        return termUpperBound;
    }

    public void setTermUpperBound(float termUpperBound) {
        this.termUpperBound = termUpperBound;
    }

    public int getnBlock() {
        return nBlock;
    }

    public long getOffsetSkipBlocks() {
        return offsetSkipBlocks;
    }

    public void setIndex(int index) {
        this.index = index;
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
