package queryProcessing;
import java.util.ArrayList;

public class ResultQueue{
    ArrayList<QueueElement> queue;
    int k;

    public ResultQueue(){
        this.queue = new ArrayList<>();
        this.k = 1;
        for(int i=0; i<k; i++)
            queue.add(new QueueElement(-1, -1));
    }

    // method to add an element into the queue
    public boolean push(QueueElement qe){
        // remove element if already present with same docId because the score could be higher in the new element
        removeElementAlreadyPresent(qe);
        for(int i=0; i<queue.size(); i++){
            // add element if found one element with score lower
            if(queue.get(i).getScore() < qe.getScore()){
                queue.add(i, qe);
                // check number of elements in the queue and eventually remove the last
                if (queue.size() == k+1) {
                    queue.remove(k);
                }
                return true;
            }
        }
        return false;
    }

    // method to remove a docId already present
    private void removeElementAlreadyPresent(QueueElement qe){
        if(queue.isEmpty())
            return;
        for(int i=0; i<k; i++){
            if(queue.get(i).getDocID() == qe.getDocID()) {
                queue.remove(i);
                return;
            }
        }
    }


}
