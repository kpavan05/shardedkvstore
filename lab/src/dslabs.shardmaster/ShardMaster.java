package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.HashMap;
import java.util.Iterator;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
    public static final int INITIAL_CONFIG_NUM = 0;

    private final int numShards;

    // TODO: declare fields for your implementation ...
    private ShardConfig curConfig;
    private Map<Integer, ShardConfig> configMap = new HashMap<>();
    public ShardMaster(int numShards) {
        this.numShards = numShards;

        // TODO: initial fields ...
        configMap.clear();
        curConfig = null;
    }

    public interface ShardMasterCommand extends Command {
    }

    @Data
    public static final class Join implements ShardMasterCommand {
        private final int groupId;
        private final Set<Address> servers;
    }

    @Data
    public static final class Leave implements ShardMasterCommand {
        private final int groupId;
    }

    @Data
    public static final class Move implements ShardMasterCommand {
        private final int groupId;
        private final int shardNum;
    }

    @Data
    public static final class Query implements ShardMasterCommand {
        private final int configNum;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    public interface ShardMasterResult extends Result {
    }

    @Data
    public static final class Ok implements ShardMasterResult {
    }

    @Data
    public static final class Error implements ShardMasterResult {
    }

    @Data
    public static final class ShardConfig implements ShardMasterResult {
        private final int configNum;

        // groupId -> <group members, shard numbers>
        private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;
    }

    private Set<Integer> createShardSet(int numShards) {
        return IntStream.rangeClosed(1, numShards).boxed()
                        .collect(Collectors.toSet());
    }

    @Override
    public Result execute(Command command) {
        if (command instanceof Join) {
            Join join = (Join) command;

            // TODO: implement Join ...
            if (curConfig != null && curConfig.groupInfo.containsKey(join.groupId)) {
                return new Error();
            }
            return executeJoin(join);
        }

        if (command instanceof Leave) {
            Leave leave = (Leave) command;

            // TODO: implement Leave ...
            if (curConfig == null || !curConfig.groupInfo.containsKey(leave.groupId)) {
                return new Error();
            }
            return executeLeave(leave);
        }

        if (command instanceof Move) {
            Move move = (Move) command;

            // TODO: implement Move ...
            if (curConfig == null || (curConfig.groupInfo.containsKey(move.groupId) &&
                    curConfig.groupInfo.get(move.groupId).getRight().contains(move.shardNum))) {
                return new Error();
            }
            if (!curConfig.groupInfo.containsKey(move.groupId)) {
                return new Error();
            }
            return executeMove(move);
        }

        if (command instanceof Query) {
            Query query = (Query) command;

            // TODO: implement Query ...
            if (curConfig == null) {
                return new Error();
            }
            if(query.configNum == -1 || query.configNum > curConfig.configNum) {
                return curConfig;
            }

            return configMap.get(query.configNum);
        }

        throw new IllegalArgumentException();
    }

    /* -------------------------------------------------------------------------
    Utils
   -----------------------------------------------------------------------*/
    // TODO: add utils here ...
    private ShardMasterResult executeJoin(Join cmd) {
        int configNum = curConfig == null ? INITIAL_CONFIG_NUM:curConfig.configNum() +1;
        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();

        if (curConfig == null) {
            Pair<Set<Address>, Set<Integer>> val = new MutablePair<>(cmd.servers(), createShardSet(numShards));
            groupInfo.put(cmd.groupId, val);
            ShardConfig config = new ShardConfig(configNum, groupInfo);
            configMap.put(configNum, config);
            curConfig = config;
            return new Ok();
        }
        int nGroups = curConfig.groupInfo.size() + 1;
        int equalShards = (numShards/nGroups);
        int remShards = numShards % nGroups;

        ArrayList<Integer> shardList = new ArrayList<>();
        LinkedList<Integer> idList = sortGroupsByShardSize();
        //for (int i : curConfig.groupInfo.keySet())
        for (int i : idList){
            Pair<Set<Address>, Set<Integer>> p = SerializationUtils.clone(curConfig.groupInfo.get(i));
            int diff = p.getRight().size() - equalShards;
            if (diff < 0 && shardList.size() > 0) {
                Iterator<Integer> itr = shardList.iterator();
                int count = p.getRight().size();
                while(itr.hasNext()) {
                    if (count == equalShards) break;
                    p.getRight().add(itr.next());
                    itr.remove();
                    count++;
                }
            } else {
                if (remShards > 0) {
                    diff--;
                    remShards--;
                }

                Iterator<Integer> itr = p.getRight().iterator();
                int count = 0;
                while (itr.hasNext()) {
                    if (diff == count)
                        break;
                    shardList.add(itr.next());
                    itr.remove();
                    count++;
                }
            }
            groupInfo.put(i, p);
        }
        Pair<Set<Address>, Set<Integer>> val = new MutablePair<>(cmd.servers(),
                shardList.stream().collect(Collectors.toSet()));
        groupInfo.put(cmd.groupId, val);

        ShardConfig config = new ShardConfig(configNum, groupInfo);
        configMap.put(configNum, config);
        curConfig = config;
        return new Ok();
    }

    private ShardMasterResult executeLeave(Leave cmd) {
        int configNum = curConfig.configNum + 1;
        Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();

        Set<Integer> leaveSet = curConfig.groupInfo.get(cmd.groupId).getRight().stream().collect(Collectors.toSet());
        Iterator<Integer> itr = leaveSet.iterator();
        int nGroups = curConfig.groupInfo.size() -1;
        int equalShards = (numShards/nGroups);
        int remShards = numShards % nGroups;

        for (Integer gid : curConfig.groupInfo.keySet()) {
            if (gid == cmd.groupId) continue;
            Pair<Set<Address>, Set<Integer>> p = SerializationUtils.clone(curConfig.groupInfo.get(gid));


            int diff = equalShards - p.getRight().size();
            if (diff > 0) {
                if (remShards > 0) {
                    diff++;
                    remShards--;
                }

                int count = 0;
                while (itr.hasNext()) {
                    if (diff == count) break;
                    p.getRight().add(itr.next());
                    itr.remove();
                    count++;
                }
            }
            groupInfo.put(gid, p);
        }

        ShardConfig config = new ShardConfig(configNum, groupInfo);
        configMap.put(configNum, config);
        curConfig = config;
        return new Ok();
    }

    private ShardMasterResult executeMove(Move cmd) {
        boolean bShardFound = false;
        int findGroup = -1;
        for (Integer gid : curConfig.groupInfo.keySet()) {
            if (curConfig.groupInfo.get(gid).getRight().contains(cmd.shardNum)) {
                //curConfig.groupInfo.get(gid).getRight().remove(cmd.shardNum);
                bShardFound = true;
                findGroup = gid;
            }
        }
        if (!bShardFound) {
            return new Error();
        }

        int configNum = curConfig.configNum + 1;
        Map<Integer,Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
        for (Integer gid : curConfig.groupInfo.keySet()) {
            Pair<Set<Address>, Set<Integer>> p = SerializationUtils.clone(curConfig.groupInfo.get(gid));
            if (gid == findGroup) {
                p.getRight().remove(cmd.shardNum);
            }
            groupInfo.put(gid, p);
        }
        groupInfo.get(cmd.groupId).getRight().add(cmd.shardNum);
        ShardConfig config = new ShardConfig(configNum, groupInfo);
        configMap.put(configNum, config);
        curConfig = config;
        return new Ok();
    }

    private LinkedList<Integer> sortGroupsByShardSize() {
        LinkedList lst = new LinkedList();
        curConfig.groupInfo.entrySet().stream().sorted(new Comparator<Entry<Integer, Pair<Set<Address>, Set<Integer>>>>() {
            @Override
            public int compare(
                    Entry<Integer, Pair<Set<Address>, Set<Integer>>> t1,
                    Entry<Integer, Pair<Set<Address>, Set<Integer>>> t2) {
                if (t1.getValue() == null || t2.getValue() == null) {
                    return  0;
                }
                if (t1.getValue().getRight().size() < t2.getValue().getRight().size()) {
                    return 1;
                } else if (t1.getValue().getRight().size() > t2.getValue().getRight().size()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        }).forEachOrdered(x -> lst.add(x.getKey()));
        return lst;
    }
}
