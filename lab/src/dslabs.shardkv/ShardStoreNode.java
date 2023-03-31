package dslabs.shardkv;

import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import dslabs.kvstore.TransactionalKVStore.Transaction;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

abstract class ShardStoreNode extends Node {
    @Getter(AccessLevel.PACKAGE) private final Address[] shardMasters;
    private final int numShards;

    ShardStoreNode(Address address, Address[] shardMasters, int numShards) {
        super(address);
        this.shardMasters = shardMasters;
        this.numShards = numShards;
    }

    void broadcastToShardMasters(Message message) {
        broadcast(message, shardMasters);
    }

    /**
     * Returns the shard number for a given key when the system has numShards
     * shards in total. The shards are numbered 1..numShards (inclusive). When
     * the key ends in \d+ (e.g. key-10), the shard number is given by that
     * number (e.g., 10 mod numShards). Otherwise, the shard number is the hash
     * value of the key (mod numShards).
     *
     * @param key
     *         the key
     * @param numShards
     *         the total number of shards in the system
     * @return the shard number of key (in 1..numShards inclusive)
     */
    static int keyToShard(@NonNull String key, int numShards) {
        LinkedList<Character> cl = new LinkedList<>();
        for (int i = key.length() - 1;
             i >= 0 && Character.isDigit(key.charAt(i)); i--) {
            cl.add(key.charAt(i));
        }
        Collections.reverse(cl);

        int hash = 0;
        if (cl.size() != 0) {
            for (char c : cl) {
                hash = hash * 10 + Character.getNumericValue(c);
            }
        } else {
            hash = key.hashCode();
        }

        int mod = hash % numShards;
        if (mod <= 0) {
            mod += numShards;
        }
        return mod;
    }

    /**
     * @see #keyToShard(String, int)
     */
    int keyToShard(String key) {
        return keyToShard(key, numShards);
    }

    int findCoordinator(Command command, ShardConfig config) {
        if (config == null || !(command instanceof Transaction)) {
            return -1;
        }
        Transaction tr = (Transaction) command;
        Set<String> keys = tr.keySet();
        int maxGroupId = 0;
        int curGroupId = 0;
        for(String s: tr.keySet()) {
            int shardNum = keyToShard(s);
            curGroupId = findShardOwner(shardNum, config);
            if (curGroupId > 0 && curGroupId > maxGroupId) {
                maxGroupId = curGroupId;
            }
        }
        return maxGroupId;
    }

    int findShardOwner(int shardId, ShardConfig config) {
        for(int gid : config.groupInfo().keySet()) {
            if (config.groupInfo().get(gid).getRight().contains(shardId)){
                return gid;
            }
        }
        return -1;
    }

}
