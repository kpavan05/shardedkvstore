package dslabs.kvstore;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import org.apache.commons.lang3.SerializationUtils;


@ToString
@EqualsAndHashCode
public class KVStore implements Application {

    public interface KVStoreCommand extends Command {
    }

    public interface SingleKeyCommand extends KVStoreCommand {
        String key();
    }

    @Data
    public static final class Get implements SingleKeyCommand {
        @NonNull private final String key;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    @Data
    public static final class Put implements SingleKeyCommand {
        @NonNull private final String key, value;
    }

    @Data
    public static final class Append implements SingleKeyCommand {
        @NonNull private final String key, value;
    }

    public interface KVStoreResult extends Result {
    }

    @Data
    public static final class GetResult implements KVStoreResult {
        @NonNull private final String value;
    }

    @Data
    public static final class KeyNotFound implements KVStoreResult {
    }

    @Data
    public static final class PutOk implements KVStoreResult {
    }

    @Data
    public static final class AppendResult implements KVStoreResult {
        @NonNull private final String value;
    }

    //TODO: declare fields for your implementation
    protected HashMap<String,String> store_;

    public KVStore() {
        store_ = new HashMap<String, String>();
    }

    // copy constructor
    public KVStore(KVStore application){
        //Hints: remember to deepcopy all fields, especially the mutable ones
        store_ = new HashMap<String, String>();
        store_.putAll(application.store_);
        //store_ = (HashMap<String, String>) application.store_.clone();
        //store_ = SerializationUtils.clone(application.store_);
        /*for (HashMap.Entry<String, String> item: application.store_.entrySet()) {
            store_.put(item.getKey(), item.getValue());
        }*/

    }

    @Override
    public KVStoreResult execute(Command command) {
        if (command instanceof Get) {
            Get g = (Get) command;
            if (store_.containsKey(g.key())) {
                return new GetResult(store_.get(g.key()));
            } else {
                 return new KeyNotFound();
            }
        }

        if (command instanceof Put) {
            Put p = (Put) command;
            store_.put(p.key(), p.value());
            return new PutOk();
        }

        if (command instanceof Append) {
            Append a = (Append) command;
            if (store_.containsKey(a.key())) {
                String newValue = store_.get(a.key()) + a.value() ;
                store_.put(a.key(), newValue);
                return new AppendResult(newValue);
            } else {
                store_.put(a.key(), a.value());
                return new AppendResult(a.value());
            }
        }

        throw new IllegalArgumentException();
    }
}
