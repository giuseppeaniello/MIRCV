package queryProcessing;

import indexing.DocumentTable;
import indexing.InvertedIndex;
import indexing.LexiconFinal;
import org.apache.hadoop.io.Text;
import indexing.Ranking;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
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
    private double score;
    private ArrayList<Integer> currentBlocks;
    private static boolean scoringFunction; // 0 means TFIDF and 1 means BM25

    public ConjunctiveQuery(int n, boolean scoringFunction){
        ConjunctiveQuery.scoringFunction = scoringFunction;
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

    // method to find the index in which add the posting list ordered by DF
    public  int findIndexToAdd(float numberToAdd){
        for(int i=0; i<dfVector.size(); i++){
            if(dfVector.get(i) >= numberToAdd)
                return i;
        }
        return dfVector.size();
    }

    // next() operation:
    // has to check cases in which there is no next document and cases in which
    // there are other documents but they are in different blocks so they have to be
    // loaded from file
    public boolean nextDocId(int index, long offsetSkipInfo, int nBlocks, FileChannel skipChannnel,
                             FileChannel docIdChannel, FileChannel tfChannel) throws IOException {
        // keep track of the currend document by deleting the previous ones
        P.get(index).remove(0);
        Ptf.get(index).remove(0);
        // case block is finished
        if(P.get(index).size()==0){
            currentBlocks.set(index,currentBlocks.get(index)+1);
            // there are no other blocks
            if(offsetSkipInfo + 32*currentBlocks.get(index) >= offsetSkipInfo+32*nBlocks){
                return true;
            }
            // there are other blocks that has to be loaded
            else {
                SkipBlock newInfo = SkipBlock.readSkipBlockFromFile(skipChannnel, offsetSkipInfo + 32*currentBlocks.get(index));
                info.set(index, newInfo);
                ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                        InvertedIndex.readDocIDsOrTFsPostingListCompressed(docIdChannel, newInfo.getoffsetDocId(),
                                newInfo.getLenBlockDocId())));
                P.set(index, docids);
                ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                        tfChannel, newInfo.getOffsetTf(), newInfo.getLenBlockTf()));
                Ptf.set(index, tfs);
                return false;
            }
        }
        return false;
    }

    // nextGEQ(docId) operation:
    // has to check cases in which there is no next document and cases in which
    // there are other documents but they are in different blocks so they have to be
    // loaded from file, this operation loads a block only if there could be the result of the nexGEQ
    // exploiting the lastDocument field in SkipInfo
    public boolean nextGEQ(int index, long value, long startSkipBlock, int nBlock ,FileChannel skipChannel,
                           FileChannel docIdChannel, FileChannel tfChannel) throws IOException {
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
                // read next SkipBlock
                SkipBlock newInfo = SkipBlock.readSkipBlockFromFile(skipChannel, startSkipBlock+32*currentBlocks.get(index));
                // check if the next block could contain the result or it has to be skipped
                if(newInfo.getFinalDocId() >= value){
                    ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                            InvertedIndex.readDocIDsOrTFsPostingListCompressed(docIdChannel, newInfo.getoffsetDocId(),
                                    newInfo.getLenBlockDocId())));
                    P.set(index, docids);
                    ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                            tfChannel, newInfo.getOffsetTf(), newInfo.getLenBlockTf()));
                    Ptf.set(index, tfs);
                    // remove documents until the result is in position 0
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
            // there are no more documents in the posting list
            return true;
        }
        return false;
    }

    // method to compute the TFIDF score
    public static double scoreTFIDF(int tf,long df){
        return ((1 + log(tf))* Ranking.idf(df));
    }

    // method to compute BM25 score
    public static double scoreBM25(int tf, float df, float dl){
        float b = 0.75F;
        float k = 1.2F;
        return ( (tf/ ((k*( (1-b)+ (b*(dl/ DocumentTable.getAverageLength())) ))+tf) ) * log(Ranking.totalNumberDocuments/df));
    }

    // method to perform the scoring based on the check inserted by the user
    public static double score(int tf, long df, float dl){
        if(scoringFunction)
            return scoreBM25(tf, df, dl);
        else
            return scoreTFIDF(tf, df);
    }

    // method to compute top k results for a query text
    public ResultQueue computeTopK(LexiconFinal lex) throws IOException {
        String pathSkipInfo;
        String pathDocId;
        String pathTF;
        // check what files to load
        if(MainQueryProcessing.getFlagStemmingAndStopWordRemoval()){
            pathSkipInfo = "SkipInfoStemmedAndStopwordRemoved";
            pathDocId = "InvertedDocIdStemmedAndStopwordRemoved";
            pathTF = "InvertedTFStemmedAndStopwordRemoved";
        }
        else{
            pathSkipInfo = "SkipInfoWithoutStemmingAndStopwordRemoving";
            pathDocId = "InvertedDocIdWithoutStemmingAndStopwordRemoving";
            pathTF = "InvertedTFWithoutStemmingAndStopwordRemoving";
        }
        RandomAccessFile invertedDocIdFile = new RandomAccessFile(new File(pathDocId), "r");
        FileChannel invDocIdsChannel = invertedDocIdFile.getChannel();
        RandomAccessFile invTfsFile = new RandomAccessFile(new File(pathTF), "r");
        FileChannel invertedTfsChannel = invTfsFile.getChannel();
        RandomAccessFile skipInfoFile = new RandomAccessFile(new File(pathSkipInfo), "r");
        FileChannel skipInfoChannel = skipInfoFile.getChannel();

        ArrayList<Integer> dfVector = new ArrayList<>();
        // Load first blocks ordered by DF
        for (Text term : lex.lexicon.keySet()){
            int index = findIndexToAdd(lex.lexicon.get(term).getDf());
            dfVector.add(index, lex.lexicon.get(term).getDf());

            // Load skip info
            info.add(index, SkipBlock.readSkipBlockFromFile(skipInfoChannel,lex.lexicon.get(term).getOffsetSkipBlocks()));

            // load first block of docIds posting list and insert it in matrix P (matrix of blocks of posting list of
            // each query term ordered by DF)
            ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                    InvertedIndex.readDocIDsOrTFsPostingListCompressed(invDocIdsChannel,info.get(index).getoffsetDocId(),
                            info.get(index).getLenBlockDocId())));
            P.add(index,docids);

            // load first block of TFs posting list and insert it in matrix P (matrix of blocks of posting list of
            // each query term ordered by DF)
            ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                    invertedTfsChannel,info.get(index).getOffsetTf(),info.get(index).getLenBlockTf()));
            Ptf.add(index,tfs);
            termOrdered.add(index,term);
        }
        current = P.get(0).get(0);
        int i = 1;
        boolean finish = false;
        while (!finish) {
            // check if a document is in all posting lists
            for ( i = 1; i<n; i++ ){
                finish = nextGEQ(i,current,lex.lexicon.get(termOrdered.get(i)).getOffsetSkipBlocks(),lex.lexicon.get(termOrdered.get(i)).getnBlock(),
                        skipInfoChannel,invDocIdsChannel,invertedTfsChannel);
                if(finish)
                    break;
                // check if skipping is needed
                if(P.get(i).get(0) > current){
                    finish = nextGEQ(0,P.get(i).get(0),lex.lexicon.get(termOrdered.get(0)).getOffsetSkipBlocks(),lex.lexicon.get(termOrdered.get(0)).getnBlock(),
                            skipInfoChannel,invDocIdsChannel,invertedTfsChannel);
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
            }
            if (i == n && !finish){
                // compute the score for a document
                for (int j = 0; j<n; j++){
                    score += score(Ptf.get(j).get(0),lex.lexicon.get(termOrdered.get(j)).getDf(), DocumentTable.getDocTab().get(P.get(j).get(0)));
                }
                // add the score into the queue
                topK.push(new QueueElement(current , score));
                score=0;
                finish = nextDocId(0,lex.lexicon.get(termOrdered.get(0)).getOffsetSkipBlocks(),lex.lexicon.get(termOrdered.get(0)).getnBlock(),
                        skipInfoChannel,invDocIdsChannel,invertedTfsChannel);
                if(finish)
                    break;
                current = P.get(0).get(0);
            }
        }
        return topK;
    }


}
