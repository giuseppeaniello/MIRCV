import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;

import static java.lang.Math.log;

public class MaxScore {

    private  ArrayList<Integer> currentBlocks;
    private int n;
    //Upload first Skip Blocks for each term
    private ArrayList<SkipBlock> info;

    //Define sigma(vector of term upperBound)
    private ArrayList<Float> sigma;
    //Define P (matrix of all first block of posting list)
    private ArrayList<ArrayList<Long>> P;
    private ArrayList<ArrayList<Integer>> Ptf;
    private float score;
    private ArrayList<Text> termOrdered;

    private float threshold;
    private int pivot;
    private long current;
    private ResultQueue topK;
    private ArrayList<Float> ub;
    private long next;
    public MaxScore(int n){
    //Contatore del blocco corrente
        this.n = n;
        this.currentBlocks = new ArrayList<>();
        for (int i = 0; i<n; i++) {
            this.currentBlocks.add(0);
        }

        //Upload first Skip Blocks for each term
        this.info = new ArrayList<>();

        //Define sigma(vector of term upperBound)
        this.sigma = new ArrayList<>();
        //Define P (matrix of all first block of posting list)
        this.P = new ArrayList<>();
        this.Ptf = new ArrayList<>();
        this.score = 0;
        this.termOrdered = new ArrayList<>();
        this.threshold = 0;
        this.pivot = 0;

        this.topK = new ResultQueue();
        this.ub = new ArrayList<>();
        this.next  = 0;
    }

    public float getScore() {
        return score;
    }

    public ArrayList<ArrayList<Integer>> getPtf() {
        return Ptf;
    }

    public ArrayList<ArrayList<Long>> getP() {
        return P;
    }

    public  ArrayList<Float> getSigma() {
        return sigma;
    }

    public ArrayList<Integer> getCurrentBlocks() {
        return currentBlocks;
    }

    public ArrayList<SkipBlock> getInfo() {
        return info;
    }

    public ArrayList<Text> getTermOrdered() {
        return termOrdered;
    }

    public int getN() {
        return n;
    }

    public long findMinDocId(){
        ArrayList<Long> tmp = new ArrayList<>();
        for(ArrayList<Long> postingList : P){
            tmp.add(postingList.get(0));
        }
        return Collections.min(tmp);
    }
    public boolean nextDocId(int index, long offsetSkipInfo, int nBlocks){
        P.get(index).remove(0);
        Ptf.get(index).remove(0);
        //Add case finish block
        if(P.get(index).size()==0){
            currentBlocks.set(index,currentBlocks.get(index)+1);
            if(offsetSkipInfo + 32*currentBlocks.get(index) >= offsetSkipInfo+32*nBlocks){
                P.remove(index);
                Ptf.remove(index);
                sigma.remove(index);
                termOrdered.remove(index);
                info.remove(index);
                n -= 1;
                currentBlocks.remove(index);
                ub.remove(index);
                return false;
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
               return true;
           }
        }
        return true;
    }
    public void nextGEQ(int index, long value, long startSkipBlock, int nBlock){
        System.out.println("ENTRAAAAAAA");
        for (int i = 0; i< P.get(index).size();i++){

            if(P.get(index).get(i) < value){
                P.get(index).remove(i);
                Ptf.get(index).remove(i);
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
                        }
                        else
                            return;
                    }
                }
            }
            // blocchi finiti per quella PostingList
            P.remove(index);
            Ptf.remove(index);
            sigma.remove(index);
            termOrdered.remove(index);
            info.remove(index);
            n -= 1;
            currentBlocks.remove(index);
            ub.remove(index);
        }
        System.out.println("EsceeeeeeeNEXTGEQ");
        //Add pigAio
    }

    public  int findIndexToAdd( float numberToAdd){
        for(int i=0; i<sigma.size(); i++){
            if(sigma.get(i) >= numberToAdd)
                return i;
        }
        return sigma.size();
    }
    public static float scoreTFIDF(int tf,long df){
        return (float) ((1 + log(tf))*Ranking.idf(df));

    }

    public ResultQueue maxScore(LexiconFinal lex) throws FileNotFoundException {
        MaxScore maxScore = new MaxScore(lex.lexicon.size());


        for (Text term : lex.lexicon.keySet()){

            //Trovo posizione ordinata dove inserire il valore
            int index = findIndexToAdd(lex.lexicon.get(term).getTermUpperBoundTFIDF());

            //Riempio sigma(Vettore term upperBound in ordine crescente)
            sigma.add(index,lex.lexicon.get(term).getTermUpperBoundTFIDF());

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

            //Variabile usate per tenere traccia del nuovo ordine
            termOrdered.add(index,term);
        }
        //Aggiungo Document Upper Bound
        ub.add(0,sigma.get(0));
        for (int i = 1; i < sigma.size(); i++)
            ub.add(ub.get(i-1)+ sigma.get(i));
        //Trova il minimo docId tra tutte le postingLIst in P
        current = findMinDocId();

        while(pivot < n && n != 0){
            score = 0;
            next = Long.MAX_VALUE;
            for(int i = pivot ;i<n ; i++) {
                if(P.get(i).get(0) == current){
                    score += scoreTFIDF(Ptf.get(i).get(0),lex.lexicon.get(termOrdered.get(i)).getDf()) ;
                    if( !nextDocId(i,lex.lexicon.get(termOrdered.get(i)).getOffsetSkipBlocks(),lex.lexicon.get(termOrdered.get(i)).getnBlock()) ){
                        if(i!=0)
                            i -= 1;
                    }
                }
                if(n != 0 && P.get(i).get(0) < next ){
                    next = P.get(i).get(0);
                }

            }

            for(int i = pivot-1; i >= 0; i--){
                if (score + ub.get(i) <= threshold)
                    break;
                nextGEQ(i, current, lex.lexicon.get(termOrdered.get(i)).getOffsetSkipBlocks(), lex.lexicon.get(termOrdered.get(i)).getnBlock());
                if(P.get(i).get(0) == current){
                     score += scoreTFIDF(Ptf.get(i).get(0),lex.lexicon.get(termOrdered.get(i)).getDf());
                }
            }
            if (topK.push(new QueueElement(current, score))){
                threshold = topK.queue.get(topK.queue.size()-1).getScore();
                while(pivot<n && ub.get(pivot)<threshold){
                    pivot +=1;
                }
            }
            current = next;


        }
        return topK;

    }
    public static void main(String arg[]) throws FileNotFoundException {
        ArrayList<Text> terms = new ArrayList<>();

        terms.add(new Text("ciao                "));
        terms.add(new Text("anna                "));
        terms.add(new Text("santi               "));

        LexiconFinal lex = Ranking.createLexiconWithQueryTerm(terms);
        MaxScore max = new MaxScore(lex.lexicon.size());
        ResultQueue qq = max.maxScore(lex);
        for(QueueElement qe : qq.queue){
            System.out.println(qe.getDocID() + " " + qe.getScore());
        }

    }


}
