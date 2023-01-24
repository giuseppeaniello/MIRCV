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
    private static int scoringFunction; // 0 means TFIDF and 1 means BM25

    public MaxScore(int n, int scoringFunction){
        this.n = n;
        this.currentBlocks = new ArrayList<>();
        MaxScore.scoringFunction = scoringFunction;
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
        String pathSkipInfo;
        String pathDocId;
        String pathTF;
        if(MainQueryProcessing.flagStopWordAndStemming == 1){
            pathSkipInfo = "SkipInfoStemmedAndStopwordRemoved";
            pathDocId = "InvertedDocIdStemmedAndStopwordRemoved";
            pathTF = "InvertedTFStemmedAndStopwordRemoved";
        }
        else{
            pathSkipInfo = "SkipInfoWithoutStemmingAndStopwordRemoving";
            pathDocId = "InvertedDocIdWithoutStemmingAndStopwordRemoving";
            pathTF = "InvertedTFWithoutStemmingAndStopwordRemoving";
        }
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
               SkipBlock newInfo = SkipBlock.readSkipBlockFromFile(pathSkipInfo, offsetSkipInfo + 32*currentBlocks.get(index));
               info.set(index, newInfo);
               ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                       InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathDocId, newInfo.getoffsetDocId(),
                               newInfo.getLenBlockDocId())));
               P.set(index, docids);
               ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                       pathTF, newInfo.getOffsetTf(), newInfo.getLenBlockTf()));
               Ptf.set(index, tfs);
               System.out.println("FINE");
               return true;
           }
        }
        return true;
    }

    public void nextGEQ(int index, long value, long startSkipBlock, int nBlock){
        String pathSkipInfo;
        String pathDocId;
        String pathTF;
        if(MainQueryProcessing.flagStopWordAndStemming == 1){
            pathSkipInfo = "SkipInfoStemmedAndStopwordRemoved";
            pathDocId = "InvertedDocIdStemmedAndStopwordRemoved";
            pathTF = "InvertedTFStemmedAndStopwordRemoved";
        }
        else{
            pathSkipInfo = "SkipInfoWithoutStemmingAndStopwordRemoving";
            pathDocId = "InvertedDocIdWithoutStemmingAndStopwordRemoving";
            pathTF = "InvertedTFWithoutStemmingAndStopwordRemoving";
        }
        System.out.println("ENTRAAAAAAA");
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
                SkipBlock newInfo = SkipBlock.readSkipBlockFromFile(pathSkipInfo, startSkipBlock+32*currentBlocks.get(index));
                if(newInfo.getFinalDocId() >= value){
                    ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                            InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathDocId, newInfo.getoffsetDocId(),
                                    newInfo.getLenBlockDocId())));
                    P.set(index, docids);
                    ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                            pathTF, newInfo.getOffsetTf(), newInfo.getLenBlockTf()));
                    Ptf.set(index, tfs);
                    for(int i=0; i<P.get(index).size(); i++){
                        if(P.get(index).get(i) < value) {
                            P.get(index).remove(i);
                            Ptf.get(index).remove(i);
                            i--;
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

    public static float scoreBM25(int tf, float df, float dl){
        float b = 0.75F;
        float k = 1.2F;
        return (float) ( (tf/ ((k*( (1-b)+ (b*(dl/DocumentTable.getAverageLength())) ))+tf) ) * log(Ranking.totalNumberDocuments/df));
    }

    public static float score(int tf, long df, float dl){
        if(scoringFunction != 1)
            return scoreTFIDF(tf, df);
        else
            return scoreBM25(tf, df, dl);
    }

    public ResultQueue maxScore(LexiconFinal lex) throws FileNotFoundException {
        MaxScore maxScore = new MaxScore(lex.lexicon.size(), scoringFunction);
        String pathSkipInfo;
        String pathDocID;
        String pathTF;
        if(MainQueryProcessing.flagStopWordAndStemming == 1){
            pathSkipInfo = "SkipInfoStemmedAndStopwordRemoved";
            pathDocID = "InvertedDocIdStemmedAndStopwordRemoved";
            pathTF = "InvertedTFStemmedAndStopwordRemoved";
        }
        else{
            pathSkipInfo = "SkipInfoWithoutStemmingAndStopwordRemoving";
            pathDocID = "InvertedDocIdWithoutStemmingAndStopwordRemoving";
            pathTF = "InvertedTFWithoutStemmingAndStopwordRemoving";
        }

        for (Text term : lex.lexicon.keySet()){
            //Trovo posizione ordinata dove inserire il valore
            int index = findIndexToAdd(lex.lexicon.get(term).getTermUpperBoundTFIDF());

            //Riempio sigma(Vettore term upperBound in ordine crescente)
            sigma.add(index,lex.lexicon.get(term).getTermUpperBoundTFIDF());

            //Calcolo skipInfo dei primi blocchi e li ordino in base a sigma
            info.add(index,SkipBlock.readSkipBlockFromFile(pathSkipInfo,lex.lexicon.get(term).getOffsetSkipBlocks()));

            //Trovo postingList primo blocco e lo inserisco nel vettore P(matrice delle postingList di ogni queryTerm ordinato in base a sigma)
            ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                    InvertedIndex.readDocIDsOrTFsPostingListCompressed(pathDocID,info.get(index).getoffsetDocId(),
                            info.get(index).getLenBlockDocId())));
            P.add(index,docids);

            //Troviamo le tf dei primi blocchi ordinati in base a sigma
            ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                    pathTF,info.get(index).getOffsetTf(),info.get(index).getLenBlockTf()));
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
                    score += score(Ptf.get(i).get(0),lex.lexicon.get(termOrdered.get(i)).getDf(), DocumentTable.getDocTab().get(P.get(i).get(0))) ;
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
                     score += score(Ptf.get(i).get(0),lex.lexicon.get(termOrdered.get(i)).getDf(), DocumentTable.getDocTab().get(P.get(i).get(0)));
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
        DocumentTable.readDocumentTable();
        terms.add(new Text("ciao                "));
        terms.add(new Text("anna                "));
        terms.add(new Text("santi               "));
        terms.add(new Text("de                  "));
        terms.add(new Text("chiamo              "));
        terms.add(new Text("mi                  "));

        LexiconFinal lex = Ranking.createLexiconWithQueryTerm(terms);
        MaxScore max = new MaxScore(lex.lexicon.size(), 1);
        ResultQueue qq = max.maxScore(lex);
        for(QueueElement qe : qq.queue){
            System.out.println(qe.getDocID() + " " + qe.getScore());
        }

    }


}
