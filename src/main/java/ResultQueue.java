import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.TreeMap;

public class ResultQueue{
    ArrayList<QueueElement> queue;
    int k;
    public ResultQueue(){
        this.queue = new ArrayList<>();
        this.k = 5;
    }

    public boolean push(QueueElement qe){
        for(int i=0; i< queue.size(); i++){
            if(queue.get(i).getScore() < qe.getScore()){
                queue.add(i, qe);
                if (queue.size() == k+1) {
                    queue.remove(k - 1);
                }
                return true;
            }
        }
        return false;
    }

}
