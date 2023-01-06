import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class DocumentTable {
    public HashMap<Long, Integer> docTab; // key=docID val=length

    public DocumentTable(){
        docTab = new HashMap<>();
    }


    // da testare
    public void saveDocumentTable(String filePath) throws FileNotFoundException {
        RandomAccessFile file = new RandomAccessFile(filePath ,"rw");
        Path fileP = Paths.get(filePath);
        ByteBuffer buffer = null;
        try (FileChannel fc = FileChannel.open(fileP, WRITE)) {
            for(Long key : docTab.keySet()){
                buffer = ByteBuffer.wrap(convertLongToByteArr(key));
                while (buffer.hasRemaining()) {
                    fc.write(buffer);
                }
                buffer.clear();
                byte[] valueByte = convertIntToByteArray(docTab.get(key));
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


    // da testare
    public static DocumentTable readDocumentTable(String filePath){
        Path fileP = Paths.get(filePath);
        DocumentTable result = new DocumentTable();
        ByteBuffer buffer = null;
        try (FileChannel fc = FileChannel.open(fileP, READ)) {
            for(int i = 0; i<fc.size(); i+=12) {
                fc.position(i);
                buffer = ByteBuffer.allocate(8);
                do {
                    fc.read(buffer);
                } while (buffer.hasRemaining());
                byte[] docIdByte = buffer.array();
                buffer.clear();
                fc.position( i+8 );
                buffer = ByteBuffer.allocate(4);
                do {
                    fc.read(buffer);
                } while (buffer.hasRemaining());
                byte[] lenDocByte = buffer.array();
                buffer.clear();

                long docId = convertByteArrToLong(docIdByte); // funzione che trasforma da byte[] a long
                int lenDoc = convertByteArrToInt(lenDocByte); //funzione che trasforma da byte[] a int
                result.docTab.put(docId, lenDoc);
            }
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return result;
    }

    // testata
    private static byte[] convertIntToByteArray(int length) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(length);
        return bb.array();
    }

    // testata
    private static long convertByteArrToLong(byte[] bytes) {
        long value = 0l;
        for (byte b : bytes) {
            value = (value << 8) + (b & 255);
        }
        return value;
    }

    // testata
    private static int convertByteArrToInt(byte[] bytes){
        int value = 0;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFF);
        }
        return value;
    }


    // testata
    public static byte[] convertLongToByteArr(long number){
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(number);
        return bb.array();
    }

    public void printDocumentTable(){
        for (Long key : docTab.keySet() ){
            System.out.println("Doc: "+ key+ " Length: "+ docTab.get(key));
        }
    }

    public static void main(String args[]){
        long a = 8000000;
        int c = 150000;
        byte[] intArr = convertIntToByteArray(c);
        byte[] longArr = convertLongToByteArr(a);

        long res1 = convertByteArrToInt(intArr);
        long res2 = convertByteArrToLong(longArr);

        System.out.println(res1);
        System.out.println(res2);

    }

}
