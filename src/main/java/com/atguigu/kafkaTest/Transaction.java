package com.atguigu.kafkaTest;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;


public class Transaction extends TwoPhaseCommitSinkFunction<String,ContentTransaction,Void> {
    ContentBuffer contentBuffer = new ContentBuffer();
     /**
     * Use default {@link ListStateDescriptor} for internal state serialization. Helpful utilities
     * for using this constructor are {@link TypeInformation#of(Class)}, {@link
     * TypeHint} and {@link TypeInformation#of(TypeHint)}.
     * Example:
     *
     * <pre>{@code
     * TwoPhaseCommitSinkFunction(TypeInformation.of(new TypeHint<State<TXN, CONTEXT>>() {}));
     * }</pre>
     *
      */
    public Transaction() {
        super(new KryoSerializer<>(ContentTransaction.class,new ExecutionConfig()), VoidSerializer.INSTANCE);
    }


    @Override
    protected void invoke(ContentTransaction transaction, String value, Context context) throws Exception {
        System.out.println("===invoke"+value+"current transaction:"+transaction.toString());
        transaction.tempContentWriter.write(value);
    }

    /**
     * begin transaction
     * @return
     * @throws Exception
     */
    @Override
    protected ContentTransaction beginTransaction() throws Exception {
        //create instantiate object based on current write data,temp object save complete transaction's data
        TempContentWriter writer = contentBuffer.createWriter(UUID.randomUUID().toString());
        //create instantiate object of transaction center
        ContentTransaction contentTransaction = new ContentTransaction(writer);
        System.out.println("begin transaction"+contentTransaction);
        return contentTransaction;
    }

    /**
     * pre-commit phrase, flush data to some place based on simulated scenario
     * After closing last phrase's preCommit, start a new transaction simultaneously and execute next phrase's checkpoint write operation
     * @param transaction
     * @throws Exception
     */
    @Override
    protected void preCommit(ContentTransaction transaction) throws Exception {
        System.out.println("=====precommit"+transaction.toString());
        //save transaction data to temporary file
        transaction.tempContentWriter.flush();
        transaction.tempContentWriter.close();
    }

    /**
     * commit a pre-commited transaction, if this method fail, Flink application will be restarted
     * @param transaction
     */
    @Override
    protected void commit(ContentTransaction transaction) {
        System.out.println("=====commit"+transaction.toString());
        String name = transaction.tempContentWriter.getName();
        Collection<String> content = contentBuffer.getValueByName(name);
        FileWriter fileWriter = null;
        try {
            FileWriter fileWriter1 = new FileWriter(new File("data/flinkTransaction/" + name));
            for (String s : content) {
                fileWriter1.write(s + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            System.out.println("finish commit===="+transaction.toString());
            //delete buffer data
            contentBuffer.clear(transaction.tempContentWriter.getName());
            try {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void abort(ContentTransaction transaction) {
        System.out.println("abort===="+transaction.toString());
        transaction.tempContentWriter.close();
        contentBuffer.clear(transaction.tempContentWriter.getName());
    }
}

class TempContentWriter{
    private final ContentBuffer contentBuffer;
    private final String name;
    //current list is used to represent multiple data under one transaction
    private final List<String> buffer  = new ArrayList();
    private boolean closed = false;
    TempContentWriter(String name,ContentBuffer contentBuffer){
        this.name = name;
        this.contentBuffer = contentBuffer;
    }

    public void flush() {
        //save pre-commit phrase's data
        contentBuffer.putContent(name,buffer);
    }

    public void close() {
        closed = true;
    }

    public String getName() {
        return name;
    }

    public TempContentWriter write(String value) {
        if(!closed){
            //data flush operation
            //add value to buffer
            buffer.add(value);
        }
        return this;
    }
}

//Temporary data of Transaction Center, simulating process of writing data to temporary file
class ContentBuffer implements Serializable {
    Map<String,List<String>> fileContent = new HashMap<>();
    public TempContentWriter createWriter(String name){
        return new TempContentWriter(name,this);
    }

    public void putContent(String name, List<String> value) {
        fileContent.put(name, value);
    }

    //get transaction's operation via transaction name
    public Collection<String> getValueByName(String name) {
        List<String> list = fileContent.get(name);
        return list;
    }

    public void add(String s) {
    }
    public void clear(String name){
        fileContent.remove(name);
    }
}

//Transaction process center
class ContentTransaction{
    TempContentWriter tempContentWriter;
    public ContentTransaction(TempContentWriter tempContentWriter){
        this.tempContentWriter = tempContentWriter;
    }

    @Override
    public String toString() {
        return "ContentTransaction{" +
                "tempContentWriter=" + tempContentWriter +
                '}';
    }
}
