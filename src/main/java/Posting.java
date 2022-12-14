public class Posting {

    private final long docID; // gamma compression da usare
    private int TF; //CONTROLLA CHE QUESTO SIA DAVVERO LONG // unary compression da usare
    // TF è la Term Frequency di un term all'interno del document

    public Posting(long docID){
        this.docID = docID;
        this.TF = 1;
    }

    public long getDocID(){
        return this.docID;
    }

    public long getTF(){
        return this.TF;
    }

    public void incrementTermFrequency(){
        this.TF += 1;
    }

    public int returnBytesNecessary(){
        return (int) Math.floor(this.TF / 8) + 1;
    }



}
