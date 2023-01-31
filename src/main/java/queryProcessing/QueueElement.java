package queryProcessing;

public class QueueElement {
    private long docID;
    private double score;

    public QueueElement(long docID, double score){
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

    public double getScore() {
        return score;
    }
}
