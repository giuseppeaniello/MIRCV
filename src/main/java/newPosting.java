public class newPosting {

    private final long docID; // gamma compression da usare
    private long TF; //CONTROLLA CHE QUESTO SIA DAVVERO LONG // unary compression da usare
    // TF è la Term Frequency di un term all'interno del document

    public newPosting(long docID){
        this.docID = docID;
        this.TF = 1;
    }

    public long getDocID(){
        return this.docID;
    }

    public long getTF(){
        return this.TF;
    }

    public void incrementDocumentFrequency(){
        this.TF += 1;
    }




}
