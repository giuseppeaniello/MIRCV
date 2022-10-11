import java.util.ArrayList;
import java.util.List;

public class PostingList {

    List<Posting> postingList;
    int numOfDocuments;

    public PostingList(){
        this.postingList = new ArrayList<>();
        this.numOfDocuments = 0;
    }

    public void addPosting(Posting pos){
        this.postingList.add(pos);
        this.numOfDocuments++;
    }

    public static void main(String[] args){
        int a = 12;
        int Pa = 1;
        int Pa2 = 5;
        int b = 4;
        int Pb = 9;
        int Pb2 = 36;

        Posting post1 = new Posting(a,Pa);
        post1.addPosition(Pa2);
        Posting post2 = new Posting(b,Pb);
        post2.addPosition(Pb2);

        PostingList PosLis = new PostingList();
        PosLis.addPosting(post1);
        PosLis.addPosting(post2);
        for( Posting p: PosLis.postingList)
            System.out.println(p.posting.keySet() + " " + p.posting.values());
    }


}
