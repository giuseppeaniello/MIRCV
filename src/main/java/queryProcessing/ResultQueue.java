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

    public boolean push(QueueElement qe){
        removeElementAlreadyPresent(qe);
        for(int i=0; i<queue.size(); i++){
            if(queue.get(i).getScore() < qe.getScore()){
                queue.add(i, qe);
                if (queue.size() == k+1) {
                    queue.remove(k);
                }
                return true;
            }
        }
        return false;
    }

    public void removeElementAlreadyPresent(QueueElement qe){
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
