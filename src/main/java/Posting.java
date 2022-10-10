import java.util.ArrayList;
import java.util.HashMap;

public class Posting {

    HashMap< Integer, ArrayList<Integer> > posting;
    int docID;

    public Posting(int docID, int position){
        this.docID = docID;
        this.posting = new HashMap<>();
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(position);
        posting.put(docID, tmp);
    }

    public void addPosition(int position){
        posting.get(docID).add(position);
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
