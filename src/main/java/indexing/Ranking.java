package indexing;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static java.lang.Math.log;

public class Ranking {

    HashMap<Long,Float> docScores;
    public static int totalNumberDocuments = 8841822;

    public Ranking(){
        this.docScores = new HashMap<>();
    }

    public static float idf (long df){
        return (float) log(totalNumberDocuments/df);
    }

    public void updateScoreTFIDF(long docId,int tf,long df){
        float tfidfWeight = 0;
        //calcolare tf del term in quel docId
        tfidfWeight = (float) ((1 + log(tf))*idf(df));
        if (!this.docScores.containsKey(docId))
            this.docScores.put(docId,tfidfWeight);
        else
            this.docScores.replace(docId, this.docScores.get(docId), this.docScores.get(docId)+tfidfWeight);
    }

    public void calculateTFIDFScoreQueryTerm( ArrayList<Long> docIds, ArrayList<Integer> tfs,long df){
        for (int i = 0; i<docIds.size(); i++){
            updateScoreTFIDF(docIds.get(i),tfs.get(i),df);
        }
    }

    public void printRankingTerm(){
        for (Long key : docScores.keySet()){
            System.out.println("DOCID: "+key+" SCORE: "+docScores.get(key));
        }
    }

    public float computeTermUpperBound(){
        float max = 0;
        for (Long key : docScores.keySet()){
            if(max< docScores.get(key))
                max = docScores.get(key);
        }
        return max;
    }

    public void updateScoreRSVBM25(long docId,float bm25){
        if (!this.docScores.containsKey(docId))
            this.docScores.put(docId,bm25);
        else
            this.docScores.replace(docId, this.docScores.get(docId), this.docScores.get(docId)+bm25);
    }

    public void computeRSVbm25(ArrayList<Long> docIds, ArrayList<Integer> tfs, DocumentTable dt, int df){
        long id;
        float bm25;
        for(int i=0; i<docIds.size(); i++){
            id = docIds.get(i);
            bm25 = calculateRSVbm25(tfs.get(i), df, dt.getAverageLength(), DocumentTable.getDocTab().get(id));
            updateScoreRSVBM25(id, bm25);
        }
    }

    public float calculateRSVbm25(int tf, float df,float averagedl, float dl){
        float b = 0.75F;
        float k = 1.2F;
        float bm25 = (float) ( (tf/ ((k*( (1-b)+ (b*(dl/averagedl)) ))+tf) ) * log(totalNumberDocuments/df));
        return bm25;
    }

    /*public ArrayList<queryProcessing.SkipBlock> uploadAllSkipInfo(long startOffset,int nBlocks){
        ArrayList<queryProcessing.SkipBlock> skipInfo = new ArrayList<>();
        for (int i = 0 ; i<nBlocks*32; i = i + 32){
            queryProcessing.SkipBlock info = queryProcessing.SkipBlock.readSkipBlockFromFile("SkipInfo",startOffset+i);
            skipInfo.add(info);
        }
        return skipInfo;
    }*/
  /*  public indexing.PostingList uploadPostingList(ArrayList<queryProcessing.SkipBlock> skipInfo){
        ArrayList<Long> docIds = new ArrayList<>();
        ArrayList<Integer> tfs = new ArrayList<>();
        for (queryProcessing.SkipBlock  info : skipInfo){
            long offsetDocids = info.getoffsetDocId();
            long offesetTfs = info.getOffsetTf();
            int lenDocIds = info.getLenBlockDocId();
            int lenTfs = info.getLenBlockTf();
            docIds.addAll( indexing.InvertedIndex.trasformDgapInDocIds(
                    indexing.InvertedIndex.decompressionListOfDocIds(
                            indexing.InvertedIndex.readDocIDsOrTFsPostingListCompressed("InvertedDocId",offsetDocids,lenDocIds))));
            tfs.addAll(indexing.InvertedIndex.decompressionListOfTfs(
                    indexing.InvertedIndex.readDocIDsOrTFsPostingListCompressed("InvertedTF",offesetTfs,lenTfs)));

        }
        indexing.PostingList postingLists = new indexing.PostingList();
        for (int i = 0; i< docIds.size(); i++){
            postingLists.postingList.put(docIds.get(i),tfs.get(i));
        }
    }*/

    public static long binarySearchTermInLexicon(Text term,FileChannel fileChannel,MappedByteBuffer buffer) throws IOException {

        //Get file channel in read-only mode
        long midpoint ;
        //Get direct byte buffer access using channel.map() operation

        long size = fileChannel.size()/50;
        long lb = 0;
        long ub = size;
        midpoint=-1;
        boolean found = false;
        while(!found){
            if(ub < lb) {
                midpoint = -1;
                break;
            }
            midpoint = lb + (ub-lb)/2;
            byte[] tmp = new byte[20];
            buffer.position((int) midpoint*50);
            buffer.get(tmp,0,20);
            Text termTmp = new Text(tmp);

            if(termTmp.compareTo(term)<0)
                lb = midpoint +1 ;
            else if (termTmp.compareTo(term)>0)
                ub = midpoint-1;
            else if (termTmp.compareTo(term) == 0)
                found = true;
            else
                System.out.println("Something in Binary Search go wrong");
        }

        return midpoint;

    }

    public static LexiconFinal createLexiconWithQueryTerm(ArrayList<Text> terms,FileChannel lexChannel) throws IOException {
        LexiconFinal lexQuery = new LexiconFinal();
        //Get direct byte buffer access using channel.map() operation
        MappedByteBuffer buffer = lexChannel.map(FileChannel.MapMode.READ_ONLY, 0, lexChannel.size());
        for (Text term:terms){
            long midpoint = binarySearchTermInLexicon(term,lexChannel,buffer);
            if (midpoint!=-1) {
                LexiconLineFinal l;
                l = LexiconLineFinal.readLineLexicon(lexChannel, midpoint * 50);
                lexQuery.lexicon.put(l.getTerm(), l.getLexiconValueFinal());
            }
        }
        return lexQuery;
    }
}
