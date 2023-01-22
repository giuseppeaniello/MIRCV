public class QueueElement {
    private long docID;
    private float score;

    public QueueElement(long docID, float score){
        this.docID = docID;
        this.score = score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public void setDocID(long docID) {
        this.docID = docID;
    }

    public long getDocID() {
        return docID;
    }

    public float getScore() {
        return score;
    }
}
