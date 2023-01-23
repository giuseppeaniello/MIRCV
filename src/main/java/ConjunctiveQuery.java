import org.apache.hadoop.io.Text;
import org.checkerframework.checker.units.qual.A;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import static java.lang.Math.log;

public class ConjunctiveQuery {
    private ArrayList<ArrayList<Long>> P;
    private ArrayList<ArrayList<Integer>> Ptf;
    private int n;
    private ResultQueue topK;

    private long current;
    private ArrayList<Integer> dfVector;

    private ArrayList<SkipBlock> info;
    private ArrayList<Text> termOrdered;
    private float score;
    private ArrayList<Integer> currentBlocks;
    public ConjunctiveQuery(int n){

        this.n = n;
        this.score = 0;
        this.Ptf = new ArrayList<>(n);
        this.P = new ArrayList<>(n);

        this.topK = new ResultQueue();

        this.current = -1;
        this.dfVector = new ArrayList<>();
        this.info = new ArrayList<>();
        this.termOrdered = new ArrayList<>();
        this.currentBlocks = new ArrayList<>();
        for(int i = 0 ; i< n; i++){
            currentBlocks.add(0);
        }
    }

    public ArrayList<ArrayList<Long>> getP() {
        return P;
    }

    public int getN() {
        return n;
    }



    public long getCurrent() {
        return current;
    }

    public ResultQueue getTopK() {
        return topK;
    }

    public void setCurrent(long current) {
        this.current = current;
    }



    public void setN(int n) {
        this.n = n;
    }

    public void setP(ArrayList<ArrayList<Long>> p) {
        P = p;
    }

    public void setTopK(ResultQueue topK) {
        this.topK = topK;
    }
    public  int findIndexToAdd( float numberToAdd){
        for(int i=0; i<dfVector.size(); i++){
            if(dfVector.get(i) >= numberToAdd)
                return i;
        }
        return dfVector.size();
    }
    public boolean nextDocId(int index, long offsetSkipInfo, int nBlocks){
        P.get(index).remove(0);
        Ptf.get(index).remove(0);
        //Add case finish block
        if(P.get(index).size()==0){
            currentBlocks.set(index,currentBlocks.get(index)+1);
            if(offsetSkipInfo + 32*currentBlocks.get(index) >= offsetSkipInfo+32*nBlocks){
                return true;
            }
            else {
                System.out.println("ENTRO");
                SkipBlock newInfo = SkipBlock.readSkipBlockFromFile("SkipInfo", offsetSkipInfo + 32*currentBlocks.get(index));
                info.set(index, newInfo);
                ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                        InvertedIndex.readDocIDsOrTFsPostingListCompressed("InvertedDocId", newInfo.getoffsetDocId(),
                                newInfo.getLenBlockDocId())));
                P.set(index, docids);
                ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                        "InvertedTF", newInfo.getOffsetTf(), newInfo.getLenBlockTf()));
                Ptf.set(index, tfs);
                System.out.println("FINE");
                return false;
            }
        }
        return false;
    }
    public boolean nextGEQ(int index, long value, long startSkipBlock, int nBlock){

        for (int i = 0; i< P.get(index).size();i++){
            if(P.get(index).get(i) < value){
                P.get(index).remove(i);
                Ptf.get(index).remove(i);
                i--;
            }
        }
        if(P.get(index).isEmpty()){
            while(currentBlocks.get(index) < nBlock){
                currentBlocks.set(index, currentBlocks.get(index)+1);
                // leggere il prossimo SkipBlock
                SkipBlock newInfo = SkipBlock.readSkipBlockFromFile("SkipInfo", startSkipBlock+32*currentBlocks.get(index));
                if(newInfo.getFinalDocId() >= value){
                    ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                            InvertedIndex.readDocIDsOrTFsPostingListCompressed("InvertedDocId", newInfo.getoffsetDocId(),
                                    newInfo.getLenBlockDocId())));
                    P.set(index, docids);
                    ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                            "InvertedTF", newInfo.getOffsetTf(), newInfo.getLenBlockTf()));
                    Ptf.set(index, tfs);
                    for(int i=0; i<P.get(index).size(); i++){
                        if(P.get(index).get(i) < value) {
                            P.get(index).remove(i);
                            Ptf.get(index).remove(i);
                            i--;
                        }
                        else
                            return false;
                    }
                }

            }
            // blocchi finiti per quella PostingList
            return true;
        }
        return false;
    }
    public static float scoreTFIDF(int tf,long df){
        return (float) ((1 + log(tf))*Ranking.idf(df));

    }
    public ResultQueue computeTopK(LexiconFinal lex){

        ArrayList<Integer> dfVector = new ArrayList<>();
        //Caricare primi blocchi posting ordinati in base al df
        for (Text term : lex.lexicon.keySet()){
            int index = findIndexToAdd(lex.lexicon.get(term).getDf());
            dfVector.add(index, lex.lexicon.get(term).getDf());

            //Calcolo skipInfo dei primi blocchi e li ordino in base a sigma
            info.add(index,SkipBlock.readSkipBlockFromFile("SkipInfo",lex.lexicon.get(term).getOffsetSkipBlocks()));

            //Trovo postingList primo blocco e lo inserisco nel vettore P(matrice delle postingList di ogni queryTerm ordinato in base a sigma)
            ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                    InvertedIndex.readDocIDsOrTFsPostingListCompressed("InvertedDocId",info.get(index).getoffsetDocId(),
                            info.get(index).getLenBlockDocId())));
            P.add(index,docids);

            //Troviamo le tf dei primi blocchi ordinati in base a sigma
            ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                    "InvertedTF",info.get(index).getOffsetTf(),info.get(index).getLenBlockTf()));
            Ptf.add(index,tfs);
            termOrdered.add(index,term);
        }

        current = P.get(0).get(0);
        int i = 1;
        boolean finish = false;
        while (!finish) {
            for ( i = 1; i<n; i++ ){
                finish = nextGEQ(i,current,lex.lexicon.get(termOrdered.get(i)).getOffsetSkipBlocks(),lex.lexicon.get(termOrdered.get(i)).getnBlock());
                if(finish)
                    break;
                if(P.get(i).get(0) > current){
                    finish = nextGEQ(0,P.get(i).get(0),lex.lexicon.get(termOrdered.get(i)).getOffsetSkipBlocks(),lex.lexicon.get(termOrdered.get(i)).getnBlock());
                    if(finish)
                        break;
                    if(P.get(0).get(0)>P.get(i).get(0)){
                        current = P.get(0).get(0);
                        i = 1;
                    }
                    else{
                        current = P.get(i).get(0);
                        i=0;
                    }
                    break;
                }
                //i = i +1;
            }
            if (i == n && !finish){
                for (int j = 0; j<n; j++){
                    score += scoreTFIDF(Ptf.get(j).get(0),lex.lexicon.get(termOrdered.get(j)).getDf());
                }
                topK.push(new QueueElement(current , score));
                score=0;
                finish = nextDocId(0,lex.lexicon.get(termOrdered.get(0)).getOffsetSkipBlocks(),lex.lexicon.get(termOrdered.get(0)).getnBlock());
                if(finish)
                    break;
                current = P.get(0).get(0);
                i = 1;
            }
        }

        return topK;
    }
    public static void main(String args[]) throws FileNotFoundException {
        ArrayList<Text> terms = new ArrayList<>();

        //terms.add(new Text("anna                "));

        //terms.add(new Text("santi               "));
        terms.add(new Text("de                  "));
        //terms.add(new Text("chiamo              "));
        terms.add(new Text("mi                  "));

        LexiconFinal lex = Ranking.createLexiconWithQueryTerm(terms);
        lex.printLexiconFinal();
        ConjunctiveQuery cq = new ConjunctiveQuery(lex.lexicon.size());
        ResultQueue qq = cq.computeTopK(lex);
        for(QueueElement qe : qq.queue){
            System.out.println(qe.getDocID() + " " + qe.getScore());
        }

    }
}
