package queryProcessing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;
import preprocessing.Preprocessing;
import indexing.*;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class TestQueryPreprocessing {
    private LinkedHashMap<Integer,String> query;
    private LinkedHashMap<Integer, ArrayList<Text>> queryPreproc;
    private LinkedHashMap<Integer,Long> result;

    public TestQueryPreprocessing(){
        this.query = new LinkedHashMap<>();
        this.queryPreproc = new LinkedHashMap<>();
        this.result = new LinkedHashMap<>();
    }
    public void readQueryFromDocument() throws IOException {
        File file = new File("queriesTest.tsv");

        LineIterator it = FileUtils.lineIterator(file,"UTF-8");
        while(it.hasNext()){
            String docCurrent = it.nextLine();
            int queryId = Integer.parseInt(docCurrent.split("\\t")[0]);
            String queryText = docCurrent.split("\\t")[1];
            query.put(queryId,queryText);
        }
    }
    public void preprocessingQuery() throws IOException {
        Preprocessing p = new Preprocessing();
        for (Integer id : query.keySet()){
            String text = query.get(id);

            ArrayList<Text> tmp = Preprocessing.preprocess(text, true);
            queryPreproc.put(id,tmp);
        }
    }
    public void saveResultOnFile() throws IOException {
        FileWriter resultFile = new FileWriter("results.tsv");


        for(Integer id : result.keySet()) {

            resultFile.write(id.toString() + "\t0\t" + result.get(id).toString() + "\t1\n");

        }
        resultFile.close();

    }
    public static void main (String []args) throws IOException {

        MainQueryProcessing.setFlagStemmingAndStopWordRemoval(true);
        TestQueryPreprocessing tqp = new TestQueryPreprocessing();
        tqp.readQueryFromDocument();
        tqp.preprocessingQuery();
        String pathDocTable;
        pathDocTable = "document_table_stemmed_and_stopword_removed";

        RandomAccessFile docTableFile = new RandomAccessFile(pathDocTable,"r");

        FileChannel docTableChannel = docTableFile.getChannel();
        DocumentTable.readDocumentTable(docTableChannel);
        String lexPath;
        lexPath="LexiconFinalStemmedAndStopWordRemoved";


        RandomAccessFile lexFile = new RandomAccessFile(lexPath,"r");
        FileChannel lexChannel = lexFile.getChannel();
        LexiconFinal lexQuery = new LexiconFinal();
        long start = System.currentTimeMillis();
        int count= 0;
        for(Integer id : tqp.queryPreproc.keySet() ) {
            count++;
            if(tqp.queryPreproc.get(id)!=null && tqp.queryPreproc.get(id).size()!=0) {
                lexQuery = Ranking.createLexiconWithQueryTerm(tqp.queryPreproc.get(id), lexChannel);
                if(count %1000 ==0)
                    System.out.println(count);
                if(!lexQuery.lexicon.isEmpty()) {
                    MaxScore dq = new MaxScore(lexQuery.lexicon.size(), MaxScore.getScoringFunction());
                    ResultQueue qq = dq.maxScore(lexQuery);
                    if(qq.queue!= null && !qq.queue.isEmpty())
                        tqp.result.put(id, qq.queue.get(0).getDocID()-1);
                    else
                        tqp.result.put(id, -1L);
                }
            }else
                tqp.result.put(id,-1L);
            lexQuery.lexicon.clear();
        }
        long end = System.currentTimeMillis();
        long time = end-start;
        System.out.println("tempo per la query: " + time + " millis");
        System.out.println("saving on file");
        tqp.saveResultOnFile();
    }
}
