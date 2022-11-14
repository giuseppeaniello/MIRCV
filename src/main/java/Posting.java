import java.io.Serializable;
import java.util.ArrayList;

public class Posting implements Serializable {

    ArrayList<Integer> positions;
    int docID;


    public Posting(int docID, int position){
        this.docID = docID;
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(position);
        this.positions = tmp;
    }

    public void addPosition(int position){
        positions.add(position);
    }


    public static void main(String[] args){
        int a = 12;
        int Pa = 1;
        int Pa2 = 5;

        Posting post1 = new Posting(a,Pa);
        post1.addPosition(Pa2);

    }
}
