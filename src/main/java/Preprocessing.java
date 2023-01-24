import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.tartarus.snowball.ext.PorterStemmer;
import org.apache.hadoop.io.Text;

public class Preprocessing {

    private static HashSet<String> stopwords ; //Get the list of stopwords

    public Preprocessing() throws IOException {
        this.stopwords=new HashSet<>();
        this.stopwords=getStopwords();
    }

    public static ArrayList<Text> preprocess(String doc_in) throws IOException {  //applies the preprocessing
        if(MainQueryProcessing.flagStopWordAndStemming==1){ //If check==1 the stemming and stopwords removal are applied
            String doc_out=doc_in;
            doc_out=textclean(doc_out); //Text cleaned and converted from ASCII to UNICODE
            doc_out=doc_out.toLowerCase(); //Text to lower case
            ArrayList<String> doc_no_sw = removeStopwords(doc_out, stopwords); //Remove the stopwords
            ArrayList<Text> doc_stemmed= stemming(doc_no_sw); //Applies the stemming to the string tokens
            return doc_stemmed;
        }
        else{
            String doc_out=doc_in;
            doc_out=textclean(doc_out); //Text cleaned and converted from ASCII to UNICODE
            doc_out=doc_out.toLowerCase(); //Text to lower case
            ArrayList<Text> doc_final= new ArrayList<Text>();
            String doc_w[]=doc_out.split("\\s+"); //split the string by space separator
            for(String i:doc_w ){
                Text new_word=cutWord(i);
                doc_final.add(new_word);}
            return doc_final;
        }
    }

    public static HashSet<String> getStopwords() throws IOException { //ritorna il dizionario di stopwords
        File file = new File("stopwords.txt");
        //String file=("C:\\Users\\Rauro\\OneDrive\\Desktop\\Uni\\Information Retrivial\\stopwords.txt");
        //File test2 = new File("C:\\Users\\edoar\\Documents\\Università\\Multim Inf Ret\\collectionReduction.tsv");
        HashSet<String> stopwords = new HashSet<>();
        LineIterator it = FileUtils.lineIterator(file,"UTF-8");
        while (it.hasNext()) {
            stopwords.add(it.nextLine());
        }

        return stopwords;
    }

    //Remove the stopwords in the document that are present in the stopword dictionary
    public static ArrayList removeStopwords(String document_in, HashSet<String> stopwords){
        if(document_in==null){
            return null;
        }
        ArrayList<String> doc_final= new ArrayList<String>();
        String doc_w[]=document_in.split("\\s+"); //split the string by space separator
        for(String i:doc_w){
            if(!stopwords.contains(i)){
                doc_final.add(i);
            }
        }
        return doc_final;
    }

    public static String textclean(String document_in) { //Convert the input document from ASCII to UNICODE
        document_in=getDocument(document_in);
        // strips off all non-ASCII characters
        document_in = document_in.replaceAll("[^\\x00-\\x7F]", "");
        // erases all the ASCII control characters
        document_in = document_in.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");
        // removes non-printable characters from Unicode
        document_in = document_in.replaceAll("\\p{C}", "");
        document_in= document_in.replaceAll("'s","");
        document_in= document_in.replaceAll("(([a-zA-Z]+)([0-9]+)([a-zA-Z]+))"," ");
        document_in= document_in.replaceAll("(([a-zA-Z]+)([-_.,#@+*Â£$%&]+)([a-zA-Z]+))"," ");
        document_in= document_in.replaceAll("([\\[\\_\\(\\)\\{\\}\\]]+)([0-9]+)","$2");
        document_in= document_in.replaceAll("([0-9]+)([\\[\\_\\(\\)\\{\\}\\]]+)","$1");
        document_in= document_in.replaceAll("([\\W])([a-zA-Z]+)"," $2");
        document_in= document_in.replaceAll("([a-zA-Z]+)([\\W])+","$1 ");
        document_in= document_in.replaceAll("[!._'?\"Â£#$,;%&=\\{\\}\\[\\]\\(\\)]","");
        //new part
        document_in= document_in.replaceAll("(^[a-zA-Z]+)([0-9]+$)","$1 ");
        document_in= document_in.replaceAll("(^[0-9]+)([a-zA-Z]+$)"," $2");
        document_in= document_in.replaceAll("(^[\\W])([a-zA-Z]+$)"," $2");
        document_in= document_in.replaceAll("(^[a-zA-Z]+)([\\W])+$","$1 ");
        //test2=test2.replaceAll("[^a-zA-Z ]","");
        document_in= document_in.replaceAll("[^a-zA-Z ]","");
        document_in= document_in.replaceAll("[  ]{2,}"," ");
        document_in= document_in.replaceAll("^[ ]","");
        //document_in= document_in.replaceAll("[!._'?]\\{{]][[}\\+\\*_^'!Â£$%()=%&]","");
        //document_in= document_in.replaceAll("[!-._'?\[["]]{{}}}]","");
        //document_in= document_in.replaceAll("[!-._'?\"]","");
        return document_in;
    }

    public static String getDocument(String row){ //return the document removing the id part
        return row=row.replaceAll("^[0-9].*\t"," ");
    }
    public static ArrayList<Text> stemming(ArrayList<String> doc_in){
        ArrayList<Text> doc_out= new ArrayList<Text>();
        PorterStemmer stemmer= new PorterStemmer();
        if(doc_in.isEmpty()){
            System.out.println("Document not found. Can't stem");
            return null;
        }
        for(int i=0;i<doc_in.size();i++){
            stemmer.setCurrent(doc_in.get(i)); //set string you need to stem
            stemmer.stem();  //stem the word
            Text new_word= cutWord(stemmer.getCurrent());  //Cut the word to MAX 20 characters
            doc_out.add(new_word);//get the stemmed word
        }
        return doc_out;
    }

    public static Text cutWord(String word){ //cut the word into 20 character lenghts if it has more and return it as a Hadoop.Text type
        if(word==null){
            System.out.println("Word not present. Cannot cut");
            return null;
        }
        if(word.length()>20) {
            word = word.substring(0, 20);
            Text new_word = new Text(word);
            return new_word;
        }
        else{
            String padded_word = String.format("%-20s", word); //Add whitespaces to words that have less than 20 char to reach max 20 characters lenght
            Text padded_textw = new Text(padded_word);

            return padded_textw;
        }
    }

    public static void main(String[] args) throws IOException {
        String text= "0	The presence of communication amid scientific minds was equally important to the success of the Manhattan Project as scientific intellect was. The only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated.";
        String text2= "8841817\tThat's chemistry  123 too! Fireworks get their color from metal compounds (also known as metal salts) packed inside. You probably know that if you burn metals in a hot flame (such as a Bunsen burner in a school laboratory), they glow with very intense colorsÃ¢\u0080\u0094 that's exactly what's happening in fireworks. AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        String test="[1123] (123) {123} [[123]] (3[2{1]})12-13-14 12/45/56 12\\?12\\?12 ciao/ [ciao ciao\\ /ciao \\ciao asd#asd asdas@asd asd12asd @Gianni Gianni@ Mario69 69Mario ";
        String test2= test;


        //REGEX TEST
/*
        test2=test2.replaceAll("(([a-zA-Z]+)([0-9]+)([a-zA-Z]+))"," ");
        test2=test2.replaceAll("(([a-zA-Z]+)([-_.,#@+*Â£$%&]+)([a-zA-Z]+))"," ");

        test2=test2.replaceAll("([\\[\\_\\(\\)\\{\\}\\]]+)([0-9]+)"," $2");

        test2=test2.replaceAll("([0-9]+)([\\[\\_\\(\\)\\{\\}\\]]+)","$1 ");
        //QUI
        //test2=test2.replaceAll("[a-zA-Z]+[\\W]+[a-zA-Z]+","");
        test2=test2.replaceAll("[0-9]+","");
        //
        test2=test2.replaceAll("(^[a-zA-Z]+)([0-9]+$)","$1 ");
        test2=test2.replaceAll("(^[0-9]+)([a-zA-Z]+$)"," $2");
        test2=test2.replaceAll("(^[\\W])([a-zA-Z]+$)"," $2");
        test2=test2.replaceAll("(^[a-zA-Z]+)([\\W])+$","$1 ");
        //test2=test2.replaceAll("[^a-zA-Z ]","");
        test2=test2.replaceAll("[^a-zA-Z ]","");
        test2=test2.replaceAll("[  ]{2,}"," ");
        test2=test2.replaceAll("^[ ]","");
*/
        String a = "x y ciao mi y chiamo giuseppe x y";
        HashSet<String> stopwords= getStopwords();
        ArrayList<Text> b = removeStopwords(a,stopwords);

        System.out.println(b);

    }


}
