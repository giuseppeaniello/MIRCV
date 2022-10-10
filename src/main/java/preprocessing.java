import java.text.Normalizer;

public class preprocessing {

    public static void preprocess (String doc_in) {  //applies the preprocessing
    String doc_out=doc_in;
    doc_out=textclean(doc_out);

        System.out.println(doc_in);
        System.out.println(doc_out);
    }

    public static String textclean(String document_in) { //Convert the input document from ASCII to UNICODE

        // strips off all non-ASCII characters
        document_in = document_in.replaceAll("[^\\x00-\\x7F]", "");

        // erases all the ASCII control characters
        document_in = document_in.replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "");

        // removes non-printable characters from Unicode
        document_in = document_in.replaceAll("\\p{C}", "");

        return document_in;
    }
        public static void main(String[] args){
        String text= "0	The presence of communication amid scientific minds was equally important to the success of the Manhattan Project as scientific intellect was. The only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated.";
        String text2= "8841817\tThat's chemistry too! Fireworks get their color from metal compounds (also known as metal salts) packed inside. You probably know that if you burn metals in a hot flame (such as a Bunsen burner in a school laboratory), they glow with very intense colorsâ\u0080\u0094 that's exactly what's happening in fireworks.";
        preprocessing.preprocess(text2);
    }


}
