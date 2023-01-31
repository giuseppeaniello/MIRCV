package indexing;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;

public class PostingList {

    public LinkedHashMap<Long, Integer> postingList; //dopo falla private

    public PostingList(long docID){
        this.postingList = new LinkedHashMap<>();
        this.postingList.put(docID, 1);
    }
    public PostingList(){
        this.postingList = new LinkedHashMap<>();
    }

    public void incrementTF(long docID){
        this.postingList.replace(docID, this.postingList.get(docID)+1);
    }

    public byte[] convertDocIDsToByteArray(){
        byte[] result = new byte[postingList.size()*8];
        ByteBuffer bb = ByteBuffer.allocate(result.length);
        for(Long docID: postingList.keySet()){
            bb.putLong(docID);
        }
        return bb.array();
    }

    public byte[] convertTFsToByteArray(){
        byte[] result = new byte[postingList.size()*4];
        ByteBuffer bb = ByteBuffer.allocate(result.length);
        for(Long docID: postingList.keySet()){
            bb.putInt(postingList.get(docID));
        }
        return bb.array();
    }

}
