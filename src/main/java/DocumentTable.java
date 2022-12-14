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
    private float averageLength;

    public DocumentTable(){
        docTab = new HashMap<>();
        averageLength = 0;
    }

    public void setAverageLength(float averageLength) {
        this.averageLength = averageLength;
    }

    public float getAverageLength() {
        return averageLength;
    }

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
            buffer = ByteBuffer.wrap(convertFloatToByteArray(averageLength));
            while (buffer.hasRemaining()) {
                fc.write(buffer);
            }
            buffer.clear();
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
    }

    public static DocumentTable readDocumentTable(String filePath){
        System.out.println("Document table letta: ");
        Path fileP = Paths.get(filePath);
        DocumentTable result = new DocumentTable();
        ByteBuffer buffer = null;
        try (FileChannel fc = FileChannel.open(fileP, READ)) {
            for(int i = 0; i<fc.size()-4; i+=12) {
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
            fc.position(fc.size()-4);
            buffer = ByteBuffer.allocate(4);
            do {
                fc.read(buffer);
            } while(buffer.hasRemaining());
            result.setAverageLength(ByteBuffer.wrap(buffer.array()).getFloat());
        } catch (IOException ex) {
            System.err.println("I/O Error: " + ex);
        }
        return result;
    }

    public void calculateAverageLength(){
        int sum=0;
        for(Long docId:this.docTab.keySet()){
            sum += docTab.get(docId);
        }
        setAverageLength(sum/ docTab.size());
    }

    private static byte[] convertIntToByteArray(int length) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(length);
        return bb.array();
    }

    private static byte[] convertFloatToByteArray(float value) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putFloat(value);
        return bb.array();
    }

    private static long convertByteArrToLong(byte[] bytes) {
        long value = 0l;
        for (byte b : bytes) {
            value = (value << 8) + (b & 255);
        }
        return value;
    }

    private static int convertByteArrToInt(byte[] bytes){
        int value = 0;
        for (byte b : bytes) {
            value = (value << 8) + (b & 0xFF);
        }
        return value;
    }

    public static byte[] convertLongToByteArr(long number){
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.putLong(number);
        return bb.array();
    }

    public void printDocumentTable(){
        for (Long key : docTab.keySet() ){
            System.out.println("Doc: "+ key+ " Length: "+ docTab.get(key));
        }
        System.out.println("Average length: " + averageLength);
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
