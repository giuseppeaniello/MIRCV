import java.util.ArrayList;

public class Posting {

    ArrayList<Integer> positions;
    int docID;
    int numberOfPositions;

    public Posting(int docID, int position){
        this.docID = docID;
        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(position);
        this.positions = tmp;
        this.numberOfPositions = 0;
    }

    public void addPosition(int position){
        positions.add(position);
        this.numberOfPositions++;
    }


    public static void main(String[] args){
        int a = 12;
        int Pa = 1;
        int Pa2 = 5;

        Posting post1 = new Posting(a,Pa);
        post1.addPosition(Pa2);

    }
}
