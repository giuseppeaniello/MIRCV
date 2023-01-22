import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;

import static java.lang.Math.log;

public class MaxScore {

    public static long findMinDocId(ArrayList<ArrayList<Long>> P){
        ArrayList<Long> tmp = new ArrayList<>();
        for(ArrayList<Long> postingList : P){
            tmp.add(postingList.get(0));
        }
        return Collections.min(tmp);
    }
    public static void nextDocId(ArrayList<ArrayList<Long>> P,ArrayList<ArrayList<Integer>> Ptf,ArrayList<SkipBlock> info,int index,long limit){
        P.get(index).remove(0);
        Ptf.get(index).remove(0);
        //Add case finish block
        if(P.get(index).size()==0){

           if(info.get(index).getoffsetDocId() == limit)
               System.out.println("FINE POSTING");
           else {
               System.out.println("ENTRO");
               SkipBlock newInfo = SkipBlock.readSkipBlockFromFile("SkipInfo", info.get(index).getoffsetDocId() + 32);
               info.set(index, newInfo);


               ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                       InvertedIndex.readDocIDsOrTFsPostingListCompressed("InvertedDocId", newInfo.getoffsetDocId(),
                               newInfo.getLenBlockDocId())));
               P.set(index, docids);

               ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                       "InvertedTF", newInfo.getOffsetTf(), newInfo.getLenBlockTf()));
               Ptf.set(index, tfs);
               System.out.println("FINE");
           }
        }
    }
    public static void nextGEQ(ArrayList<ArrayList<Long>> P,ArrayList<ArrayList<Integer>> Ptf, int index, long value){
        for (int i = 0; i< P.get(index).size();i++){
            if(P.get(index).get(i) < value){
                P.get(index).remove(i);
                Ptf.get(index).remove(i);
            }
        }
        //Add pigAio
    }

    public static int findIndexToAdd(ArrayList<Float> list, float numberToAdd){
        for(int i=0; i<list.size(); i++){
            if(list.get(i) >= numberToAdd)
                return i;
        }
        return list.size();
    }
    public static float scoreTFIDF(long docId,int tf,long df){
        return (float) ((1 + log(tf))*Ranking.idf(df));

    }

    public static ResultQueue maxScore(LexiconFinal lex) throws FileNotFoundException {



       //Upload first Skip Blocks for each term
        ArrayList<SkipBlock> info = new ArrayList<>();
        for(Text term : lex.lexicon.keySet()){
            long offSkip = lex.lexicon.get(term).getOffsetSkipBlocks();
            info.add(SkipBlock.readSkipBlockFromFile("SkipInfo",offSkip));
        }
        //Define sigma(vector of term upperBound)
        ArrayList<Float> sigma = new ArrayList<>();
        //Define P (matrix of all first block of posting list)
        ArrayList<ArrayList<Long>> P = new ArrayList<>();
        ArrayList<ArrayList<Integer>> Ptf = new ArrayList<>();
        float score = 0;
        ArrayList<Text> termOrdered = new ArrayList<>();
        int indexInfo = 0;
        for (Text term : lex.lexicon.keySet()){
            int index = findIndexToAdd(sigma,lex.lexicon.get(term).getTermUpperBoundTFIDF());
            sigma.add(index,lex.lexicon.get(term).getTermUpperBoundTFIDF());
            ArrayList<Long> docids = InvertedIndex.trasformDgapInDocIds(InvertedIndex.decompressionListOfDocIds(
                    InvertedIndex.readDocIDsOrTFsPostingListCompressed("InvertedDocId",info.get(indexInfo).getoffsetDocId(),
                            info.get(indexInfo).getLenBlockDocId())));
            P.add(index,docids);

            ArrayList<Integer> tfs = InvertedIndex.decompressionListOfTfs(InvertedIndex.readDocIDsOrTFsPostingListCompressed(
                    "InvertedTF",info.get(indexInfo).getOffsetTf(),info.get(indexInfo).getLenBlockTf()));
            Ptf.add(index,tfs);

            termOrdered.add(index,term);

            indexInfo ++;
        }


        ArrayList<Float> ub = new ArrayList<>();
        ub.add(0,sigma.get(0));
        for (int i = 1; i < sigma.size(); i++)
            ub.add(ub.get(i-1)+ sigma.get(i));

        float threshold = 0;
        int pivot = 0;
        long current = findMinDocId(P);
        int n = sigma.size();
        ArrayList<Long> limits = new ArrayList<>();
        for (int i = 0 ; i<n; i++ ){
            limits.add(info.get(i).getoffsetDocId()+32*lex.lexicon.get(termOrdered.get(i)).getnBlock());
        }


        ResultQueue topK = new ResultQueue();


        while(pivot<n &&  current != -1){
            System.out.println("AIA");
            score = 0;
            long next = Long.MAX_VALUE;
            for(int i = pivot ;i<n ; i++) {
                if(P.get(i).get(0) == current){

                    score = scoreTFIDF(P.get(i).get(0),Ptf.get(i).get(0),lex.lexicon.get(termOrdered.get(i)).getDf()) ;
                    nextDocId(P,Ptf,info,i,limits.get(i));
                }
                if(P.get(i).get(0) < next){
                    next =P.get(i).get(0);
                }
            }

            for(int i = pivot-1; i >= 0; i--){
                if (score + ub.get(i) <= threshold)
                    break;
                nextGEQ(P,Ptf,i,current);
                if(P.get(i).get(0) == current){
                     score += scoreTFIDF(P.get(i).get(0),Ptf.get(i).get(0),lex.lexicon.get(termOrdered.get(i)).getDf());
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
        ResultQueue qq = maxScore(lex);
    }


}
