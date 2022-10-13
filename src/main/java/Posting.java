import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Posting implements Serializable {

    HashMap<Integer, ArrayList<Integer>> posting;
    int docID;
    int numberOfPositions;

    public Posting(int docID, int position){
        this.docID = docID;
        this.posting = new HashMap<>();
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(position);
        this.posting.put(docID, tmp);
        this.numberOfPositions = 0;
    }

    public void addPosition(int position){
        posting.get(docID).add(position);
        this.numberOfPositions++;
    }

    public static void main(String[] args){
        int a = 12;
        int Pa = 1;
        int Pa2 = 5;

        Posting post1 = new Posting(a,Pa);
        post1.addPosition(Pa2);

        System.out.println(post1.posting.keySet() + " " + post1.posting.values());
    }
}
