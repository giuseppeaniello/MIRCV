package queryProcessing;

import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import static java.lang.Math.log;
import indexing.*;
public class MaxScore {

    private  ArrayList<Integer> currentBlocks;
    private int n;
    private ArrayList<SkipBlock> info;
    private ArrayList<Float> sigma;
    private ArrayList<ArrayList<Long>> P;
    private ArrayList<ArrayList<Integer>> Ptf;
    private double score;
    private ArrayList<Text> termOrdered;
    private double threshold;
    private int pivot;
    private long current;
    private ResultQueue topK;
    private ArrayList<Float> ub;
    private long next;
    private static boolean scoringFunction; // 0 means TFIDF and 1 means BM25

    public static boolean getScoringFunction() {
        return scoringFunction;
    }

    public MaxScore(int n, boolean scoringFunction){
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
        //Define P (matrix of all first block of docId posting list)
        this.P = new ArrayList<>();
        //Define Ptf (matrix of all first block of TF posting list)
        this.Ptf = new ArrayList<>();
        this.score = 0;
        this.termOrdered = new ArrayList<>();
        this.threshold = -Float.MAX_VALUE;
        this.pivot = 0;
        this.topK = new ResultQueue();
        this.ub = new ArrayList<>();
        this.next  = 0;
    }

    // method to find the minimum docId among the first posting of each posting list in P
    public long findMinDocId(){
        ArrayList<Long> tmp = new ArrayList<>();
        for(ArrayList<Long> postingList : P){
            tmp.add(postingList.get(0));
        }
        return Collections.min(tmp);
    }

    // next() operation:
    // has to check cases in which there is no next document and cases in which
    // there are other documents but they are in different blocks so they have to be
    // loaded from file
    public boolean nextDocId(int index, long offsetSkipInfo, int nBlocks,FileChannel skipChannel,
                             FileChannel docIdChannel, FileChannel tfChannel) throws IOException {
        // skip to the next document by removing the current one
        P.get(index).remove(0);
        Ptf.get(index).remove(0);
        // case there are no more postings in this block
        if(P.get(index).size()==0){
            currentBlocks.set(index,currentBlocks.get(index)+1);
            // case there are no more blocks to load
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
            // case there is at least one block to load
           else {
               SkipBlock newInfo = SkipBlock.readSkipBlockFromFile(skipChannel, offsetSkipInfo + 32*currentBlocks.get(index));
               info.set(index, newInfo);
               ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                       InvertedIndex.readDocIDsOrTFsPostingListCompressed(docIdChannel, newInfo.getoffsetDocId(),
                               newInfo.getLenBlockDocId())));
               P.set(index, docids);
               ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                       tfChannel, newInfo.getOffsetTf(), newInfo.getLenBlockTf()));
               Ptf.set(index, tfs);
               return true;
           }
        }
        return true;
    }

    // nextGEQ(docId) operation:
    // has to check cases in which there is no next document and cases in which
    // there are other documents but they are in different blocks so they have to be
    // loaded from file, this operation loads a block only if there could be the result of the nexGEQ
    // exploiting the lastDocument field in SkipInfo
    public boolean nextGEQ(int index, long value, long startSkipBlock, int nBlock,FileChannel skipChannel,
                        FileChannel docIdChannel, FileChannel tfChannel) throws IOException {
        // remove all the postings with docId < value
        for (int i = 0; i< P.get(index).size();i++){
            if(P.get(index).get(i) < value){
                P.get(index).remove(i);
                Ptf.get(index).remove(i);
                i--;
            }
        }
        // case there are no more postings in the block
        if(P.get(index).isEmpty()){
            // while there are blocks to load
            while(currentBlocks.get(index) < nBlock){
                currentBlocks.set(index, currentBlocks.get(index)+1);
                // load next SkipBlock in memory
                SkipBlock newInfo = SkipBlock.readSkipBlockFromFile(skipChannel, startSkipBlock+32*currentBlocks.get(index));
                if(newInfo.getFinalDocId() >= value){
                    ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                            InvertedIndex.readDocIDsOrTFsPostingListCompressed(docIdChannel, newInfo.getoffsetDocId(),
                                    newInfo.getLenBlockDocId())));
                    P.set(index, docids);
                    ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                            tfChannel, newInfo.getOffsetTf(), newInfo.getLenBlockTf()));
                    Ptf.set(index, tfs);
                    // remove all the postings with docId < value
                    for(int i=0; i<P.get(index).size(); i++){
                        if(P.get(index).get(i) < value) {
                            P.get(index).remove(i);
                            Ptf.get(index).remove(i);
                            i--;
                        }
                        else
                            return true;
                    }
                }
            }
            // there are no more blocks to load
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
        return true;
    }

    // method to find the position in P in which load the block
    // in order to keep posting list ordered by term upper bound
    public  int findIndexToAdd(float numberToAdd){
        for(int i=0; i<sigma.size(); i++){
            if(sigma.get(i) >= numberToAdd)
                return i;
        }
        return sigma.size();
    }

    // method to compute the TFIDF score
    public static double scoreTFIDF(int tf,long df){
        return ((1 + log(tf))* Ranking.idf(df));
    }

    // method to compute the BM25 score
    public static double scoreBM25(int tf, float df, int dl){
        float b = 0.75F;
        float k = 1.2F;
        return  ( (tf/ ((k*( (1-b)+ (b*(dl/ DocumentTable.getAverageLength())) ))+tf) ) * log(Ranking.totalNumberDocuments/df));
    }

    // method to compute the score based on the scoring function chose by the user
    public static double score(int tf, long df, int dl){
        if(scoringFunction)
            return scoreBM25(tf, df, dl);
        else
            return scoreTFIDF(tf, df);
    }

    // method used to find the top k documents in the disjunctive query case
    public ResultQueue maxScore(LexiconFinal lex) throws IOException {
        String pathSkipInfo;
        String pathDocID;
        String pathTF;
        // check the flag to find right files to read
        if(MainQueryProcessing.getFlagStemmingAndStopWordRemoval()){
            pathSkipInfo = "SkipInfoStemmedAndStopwordRemoved";
            pathDocID = "InvertedDocIdStemmedAndStopwordRemoved";
            pathTF = "InvertedTFStemmedAndStopwordRemoved";
        }
        else{
            pathSkipInfo = "SkipInfoWithoutStemmingAndStopwordRemoving";
            pathDocID = "InvertedDocIdWithoutStemmingAndStopwordRemoving";
            pathTF = "InvertedTFWithoutStemmingAndStopwordRemoving";
        }
        //Define file where is saved the index
        RandomAccessFile invertedDocIdFile = new RandomAccessFile(new File(pathDocID), "r");
        FileChannel invDocIdsChannel = invertedDocIdFile.getChannel();
        RandomAccessFile invTfsFile = new RandomAccessFile(new File(pathTF), "r");
        FileChannel invertedTfsChannel = invTfsFile.getChannel();
        RandomAccessFile skipInfoFile = new RandomAccessFile(new File(pathSkipInfo), "r");
        FileChannel skipInfoChannel = skipInfoFile.getChannel();
        for (Text term : lex.lexicon.keySet()){
            //Find the ordered position where the value is inserted
            int index = findIndexToAdd(lex.lexicon.get(term).getTermUpperBoundTFIDF());
            //Fill sigma vector(term upper bound sorted in ascending order)
            sigma.add(index,lex.lexicon.get(term).getTermUpperBoundTFIDF());
            //compute skipInfo of the first block and sorted by sigma
            info.add(index, SkipBlock.readSkipBlockFromFile(skipInfoChannel,lex.lexicon.get(term).getOffsetSkipBlocks()));
            //Find the first posting list of the block and insert in the vector P(matrix of PostingList of each queryTerm sorted by sigma)
            ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                    InvertedIndex.readDocIDsOrTFsPostingListCompressed(invDocIdsChannel,info.get(index).getoffsetDocId(),
                            info.get(index).getLenBlockDocId())));
            P.add(index,docids);
            //Find tfs postingLists sorted by sigma
            ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                    invertedTfsChannel,info.get(index).getOffsetTf(),info.get(index).getLenBlockTf()));
            Ptf.add(index,tfs);
            //variable used to save the term order
            termOrdered.add(index,term);
        }
        //Adding Document Upper Bound
        ub.add(0,sigma.get(0));
        for (int i = 1; i < sigma.size(); i++)
            ub.add(ub.get(i-1)+ sigma.get(i));
        //Find min among all the postingLists in P
        current = findMinDocId();
        while(pivot < n && n != 0){
            score = 0;
            next = Long.MAX_VALUE;
            for(int i = pivot ;i<n ; i++) {
                if(P.get(i).get(0) == current){
                    score += score(Ptf.get(i).get(0),lex.lexicon.get(termOrdered.get(i)).getDf(), DocumentTable.getDocTab().get(P.get(i).get(0))) ;
                    if( !nextDocId(i,lex.lexicon.get(termOrdered.get(i)).getOffsetSkipBlocks(),lex.lexicon.get(termOrdered.get(i)).getnBlock(),
                            skipInfoChannel,invDocIdsChannel,invertedTfsChannel) ){
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
                if(! nextGEQ(i, current, lex.lexicon.get(termOrdered.get(i)).getOffsetSkipBlocks(), lex.lexicon.get(termOrdered.get(i)).getnBlock(),
                        skipInfoChannel,invDocIdsChannel,invertedTfsChannel)) {
                    i--;
                    continue;
                }
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
}
