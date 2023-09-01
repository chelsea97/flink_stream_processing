package com.atguigu.kafkaTest;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.UUID;


public class Transaction extends TwoPhaseCommitSinkFunction<String,ContentTransaction,Void> {
    ContentBuffer buffer = new ContentBuffer();
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

    }

    /**
     * begin transaction
     * @return
     * @throws Exception
     */
    @Override
    protected ContentTransaction beginTransaction() throws Exception {
        //create instantiate object based on current write data,temp object save complete transaction's data
        TempContentWriter writer = buffer.createWriter(UUID.randomUUID().toString());
        ContentTransaction contentTransaction = new ContentTransaction(writer);
        System.out.println("begin transaction"+contentTransaction);
        return contentTransaction;
    }

    @Override
    protected void preCommit(ContentTransaction transaction) throws Exception {

    }

    @Override
    protected void commit(ContentTransaction transaction) {

    }

    @Override
    protected void abort(ContentTransaction transaction) {

    }
}

class TempContentWriter{
    private final ContentBuffer contentBuffer;
    private final String name;
    TempContentWriter(String name,ContentBuffer contentBuffer){
        this.name = name;
        this.contentBuffer = contentBuffer;
    }
}

//Temporary data of Transaction Center, simulating process of writing data to temporary file
class ContentBuffer{
    public TempContentWriter createWriter(String name){
        return new TempContentWriter(name,this);
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
