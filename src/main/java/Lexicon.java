import org.apache.hadoop.io.Text;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class Lexicon {


    LinkedHashMap<Text,LexiconValue> lexicon;
    int indexOfFile;
    long currentOffset;

    public Lexicon(int indexOfFile){
        lexicon             = new LinkedHashMap<>();
        this.indexOfFile    = indexOfFile;
        this.currentOffset  = 0;
    }

    public void addElement(Text term, long docId, InvertedIndex invInd){
        if(!lexicon.containsKey(term)){ // case new term
            LexiconValue lexiconValue = new LexiconValue(currentOffset, docId);
            invInd.addPostingOfNewTerm(currentOffset, docId);
            lexicon.put(term,lexiconValue);
            currentOffset += 1;
            lexiconValue.setLastDocument(docId);
            updateAllOffsetsInList();
        }
        else{ // case term already appeared in the same document
            if(lexicon.get(term).getLastDocument() == docId){
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                invInd.incrementPostingTF(lexicon.get(term).getOffsetInList(), docId, lexicon.get(term).getDf());
            }
            else{ // case term already appeared but in different document
                lexicon.get(term).setCf(lexicon.get(term).getCf() + 1);
                lexicon.get(term).setDf(lexicon.get(term).getDf() + 1);
                invInd.addPostingOfExistingTerm(lexicon.get(term).getOffsetInList(), docId-lexicon.get(term).getLastDocument(), lexicon.get(term).getDf());
                lexicon.get(term).setLastDocument(docId);
                currentOffset += 1;
                updateAllOffsetsInList();
            }
        }
    }

    public void sortLexicon(){
        //non va bene
        List<Text> sortedKeys = new ArrayList<>();
        for(Text term : lexicon.keySet())
            sortedKeys.add(term);

        sortedKeys.sort(null);
        LinkedHashMap<Text, LexiconValue> sortedLexicon = new LinkedHashMap<>();
        for(Text term : sortedKeys){
            sortedLexicon.put(term, lexicon.get(term));
            lexicon.remove(term);
        }
        lexicon = sortedLexicon;
        sortedLexicon = null;
        sortedKeys = null;
        System.gc();
    }

    public void saveLexiconOnFile(String filePath, int indexOfFile) throws FileNotFoundException {
        RandomAccessFile file = new RandomAccessFile(filePath ,"rw");
        Path fileP = Paths.get(filePath );
        ByteBuffer buffer = null;
        try (FileChannel fc = FileChannel.open(fileP, WRITE)) {
            for(Text key : lexicon.keySet()){
                buffer = ByteBuffer.wrap(key.getBytes());
                while (buffer.hasRemaining()) {
                    fc.write(buffer);
                }
                buffer.clear();
                byte[] valueByte = transformValueToByte(lexicon.get(key).getCf(), lexicon.get(key).getDf(),
                                                            lexicon.get(key).getOffsetInList(),
                                                                lexicon.get(key).getOffsetTF(),
                                                                    lexicon.get(key).getLenOfDocID(),
                                                                            lexicon.get(key).getLenOfTF());
                buffer = ByteBuffer.wrap(valueByte);
                while (buffer.hasRemaining()) {
                    fc.write(buffer);
                }
                buffer.clear();
            }
        } catch (IOException ex) {
        System.err.println("I/O Error: " + ex);
        }
    }

    private byte[] transformValueToByte( int cF, long dF, long offsetDocId, long offsetTF, int lenOfDocID,int lenOfTF ) {

        ByteBuffer bb = ByteBuffer.allocate(36);
        bb.putInt(cF);
        bb.putLong(dF);
        bb.putLong(offsetDocId);
        bb.putLong(offsetTF);
        bb.putInt(lenOfDocID);
        bb.putInt(lenOfTF);

        return bb.array();
    }

    public LexiconValue transformByteToValue(byte[] value){
        LexiconValue lexValue = new LexiconValue(0,0);//Vedere che valori mettere

        int count = 0;
        for (byte b : value) {
            if(count<4)
                lexValue.setCf((lexValue.getCf() << 8) + (b & 0xFF));
            else if(count <12 && count>=4)
                lexValue.setDf((lexValue.getDf() << 8) + (b & 0xFF));
            else if(count <20 && count>=12)
                lexValue.setOffsetDocID((lexValue.getOffsetDocID() << 8) + (b & 0xFF));
            else if(count<28 && count >= 20)
                lexValue.setOffsetTF((lexValue.getOffsetTF() << 8) + (b & 0xFF));
            else if(count<32 && count >= 28)
                lexValue.setLenOfDocID((lexValue.getLenOfDocID() << 8) + (b & 0xFF));
            else
                lexValue.setLenOfTF((lexValue.getLenOfTF() << 8) + (b & 0xFF));
            count ++;
        }

        return lexValue;
    }

    public void updateAllOffsetsInList(){ // questo non va chiamato, è già nel metodo che aggiunge gli elementi
        boolean first = true;  // non lo faccio private perchè magari dopo il merging ci serve anche chiamarlo a parte
        long offset = 0;
        long df = 0;
        for(Text term : lexicon.keySet()){
            if(first){
                df = lexicon.get(term).getDf();
                first = false;
            }
            else{
                lexicon.get(term).setOffsetInList(offset + df);
                df = lexicon.get(term).getDf();
                offset = lexicon.get(term).getOffsetInList();
            }
        }
    }

    public void updateAllOffsetsTF(InvertedIndex invInd){ // to be done just one time before the sorting before the saving on file
        boolean first = true;
        long offsetTF = 0;
        int lenOffTF  = 0;
        long df = 0; // df of the previous term
        for(Text term : lexicon.keySet()){
            if(first){ //the first time the offsetTF is already 0
                df = lexicon.get(term).getDf();
                for(int i=0; i<df; i++){ // df indicates the number of posting, for each posting we will need a certain number of bytes
                    offsetTF += (long) Math.floor(invInd.allPostingLists.get((int)lexicon.get(term).getOffsetInList() + i).getTF() / 8)+1;
                }
                this.lexicon.get(term).setLenOfTF((int)offsetTF);
                first = false;
            }
            else{
                lenOffTF = 0;
                lexicon.get(term).setOffsetTF(offsetTF);
                df = lexicon.get(term).getDf();
                for(int i=0; i<df; i++){
                    lenOffTF += (long) Math.floor(invInd.allPostingLists.get((int)lexicon.get(term).getOffsetInList() + i).getTF() / 8) + 1;
                    // offsetTF += (long) Math.floor(invInd.allPostingLists.get((int)lexicon.get(term).getOffsetInList() + i).getTF() / 8) + 1;
                }
                offsetTF += lenOffTF;
                this.lexicon.get(term).setLenOfTF(lenOffTF);
            }
        }
    }


    public void clearLexicon(){
        this.lexicon.clear();
        this.lexicon = null;
        this.currentOffset = 0;
        this.indexOfFile = 0;
        System.gc();
    }

    public void readLexicon(String filePath,int startReadingPosition){

        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        try (FileChannel fc = FileChannel.open(fileP, READ))
        {
            fc.position(startReadingPosition);
            buffer = ByteBuffer.allocate(22); //50 is the total number of bytes to read a complete term of the lexicon
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            buffer.clear();
            System.out.println("Term: " + new Text(buffer.array()));
            fc.position(startReadingPosition+22);
            buffer = ByteBuffer.allocate(36); //50 is the total number of bytes to read a complete term of the lexicon
            do {
                fc.read(buffer);
            } while (buffer.hasRemaining());
            buffer.clear();
            LexiconValue lexVal = transformByteToValue(buffer.array());
            System.out.println(lexVal.getCf()+" "+lexVal.getDf()+" "+lexVal.getOffsetDocID()+" "+lexVal.getOffsetTF()+
                                " "+ lexVal.getLenOfDocID()+" "+lexVal.getLenOfTF());
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
    }



    /*
    public void mergeBlocks(String path){
        // Step base:
        // apri blocco Lex0
        // apri blocco Lex1
        // apri blocco TF0
        // apri blocco TF1
        // apri blocco DocID0
        // apri blocco DocID1
        // apri nuovo file NewLex0
        // apri nuovo file NewTF0
        // apri nuovo file NewDocID0
        // leggi prima parola Lex0 e Lex1
        // inizializzati currentOffsetTF = 0
        // inizializzati currentOffsetDocID = 0
        // fino all'ultima riga di entrambi i file:
            // confronta le parole
            // se le parole sono diverse:
                    // scrivi la minore in NewLex0
                    // mantieni stesse CF e DF
                    // scrivi il posting in NewTF0 in posizione currentOffsetTF
                    // scrivi il posting in NewDocID0 in posizione currentOffsetDocID
                    // lenOffsetTF e lenOffsetDocID rimangono le stesse
                    // incrementa currentOffsetTF di una quantità pari a lenOffsetTF della parola scritta
                    // incrementa currentOffsetDocID di una quantità pari a lenOffsetDocID della parola scritta
                    // nel file che conteneva la parola minore avanza l'iteratore alla parola successiva
            // se le due parole sono uguali:
                    // scrivi la parola in NewLex0
                    // le CF si sommano
                    // le DF si sommano
                    // scrivi il posting in NewTF0 in posizione currentOffsetTF
                    // scrivi il posting in NewDocID0 in posizione currentOffsetDocID
                    // le lenOffsetTF e lenOffsetDocID si sommano
                    // incrementa currentOffsetTF di una quantità pari a lenOffsetTF della parola scritta (cioè la somma delle 2)
                    // incrementa currentOffsetDocID di una quantità pari a lenOffsetDocID della parola scritta (cioè la somma delle 2)
                    // incrementa iteratore sia di Lex0 che di Lex1
            // Step iterativo
            // ripeti tutti gli step prima prendendo sta volta NewLex0 e Lex2 e quindi creando NewLex1
            // ripeti prendendo NewLex1 e Lex3 e quindi creando NewLex2
            // e così via
            // cioè dopo aver fatto il passo base avrai:
            // for(K=0; K<numOfBlocks; K++){
            //      fai tutti gli step con NewLexK e Lex(K+2)
            // }
    }
        */
    public static void main (String[] arg) throws IOException {
        Lexicon lex = new Lexicon(0);
        InvertedIndex invInd = new InvertedIndex(0);
        // 0,2,4

        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 1, invInd);
        lex.addElement(new Text("b                   "), 22, invInd);
        lex.addElement(new Text("b                   "), 25, invInd);
        lex.addElement(new Text("b                   "), 36 , invInd);


        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);
        lex.addElement(new Text("c                   "), 1, invInd);

        lex.addElement(new Text("a                   "), 1, invInd);
        //lex.saveLexicon("prova", 0);
        lex.updateAllOffsetsInList();
        lex.updateAllOffsetsTF(invInd);
        invInd.compressListOfDocIDsAndAssignOffsetsDocIDs(lex);
        lex.sortLexicon();
       // lex.saveLexiconOnFile("LEX1",1);
      //  System.out.println("---------------------------------");
        for(Text term : lex.lexicon.keySet()){
            //prova anche a scorrere i posting
            System.out.print(term + "  ");
            System.out.print("offsetTF: " + lex.lexicon.get(term).getOffsetTF() + "  ");
            System.out.print("offsetList: " + lex.lexicon.get(term).getOffsetInList() + "  ");
            System.out.print("offsetDocID " + lex.lexicon.get(term).getOffsetDocID() + "  ");
            System.out.print("length offsetDocID " + lex.lexicon.get(term).getLenOfDocID() + "  ");
            System.out.print("length offsetTF " + lex.lexicon.get(term).getLenOfTF() + "  ");
            System.out.print("CF: " + lex.lexicon.get(term).getCf() + "  ");
            System.out.print("DF: " + lex.lexicon.get(term).getDf() + "  ");

            for(int i=0; i<lex.lexicon.get(term).getDf(); i++){
                System.out.print("docID: " + invInd.allPostingLists.get((int)lex.lexicon.get(term).getOffsetInList() + i).getDocID() + "  ");
                System.out.print("TF: " + invInd.allPostingLists.get((int)lex.lexicon.get(term).getOffsetInList() + i).getTF() + "  ");
            }
            System.out.print("\n");
        }


        byte[] compressedListOfDocID = invInd.compressListOfDocIDsAndAssignOffsetsDocIDs(lex);
        ArrayList<Integer> listDecompressed = invInd.decompressionListOfDocIds(compressedListOfDocID);
        for(Integer docID:listDecompressed)
            System.out.println(docID);
        //lex.saveLexiconOnFile("Lexicon_number_1", 0);
       // lex.readLexicon("Lexicon_number_1", 58);
        boolean[] boolArr = invInd.fromByteArrToBooleanArr(compressedListOfDocID);
      /*  for(boolean b:boolArr){
            if(b)
                System.out.print("1 ");
            else
                System.out.print("0 ");
        } */
    }
}
