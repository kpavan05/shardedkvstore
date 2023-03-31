package dslabs.shardkv;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Result;
import dslabs.framework.testing.Workload;
import dslabs.framework.testing.junit.PrettyTestName;
import dslabs.framework.testing.junit.RunTests;
import dslabs.framework.testing.junit.SearchTests;
import dslabs.framework.testing.junit.TestPointValue;
import dslabs.framework.testing.junit.UnreliableTests;
import dslabs.framework.testing.search.Search;
import dslabs.kvstore.TransactionalKVStore.MultiGetResult;
import dslabs.kvstore.TransactionalKVStoreWorkload;
import dslabs.shardmaster.ShardMaster.Join;
import dslabs.shardmaster.ShardMaster.Leave;
import dslabs.shardmaster.ShardMaster.Ok;
import java.util.List;
import java.util.Objects;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import static dslabs.framework.testing.StatePredicate.CLIENTS_DONE;
import static dslabs.framework.testing.StatePredicate.RESULTS_OK;
import static dslabs.framework.testing.StatePredicate.resultsHaveType;
import static dslabs.framework.testing.StatePredicate.statePredicate;
import static dslabs.kvstore.KVStoreWorkload.KEY_NOT_FOUND;
import static dslabs.kvstore.TransactionalKVStoreWorkload.MULTI_GETS_MATCH;
import static dslabs.kvstore.TransactionalKVStoreWorkload.OK;
import static dslabs.kvstore.TransactionalKVStoreWorkload.multiGet;
import static dslabs.kvstore.TransactionalKVStoreWorkload.multiGetResult;
import static dslabs.kvstore.TransactionalKVStoreWorkload.multiPut;
import static dslabs.kvstore.TransactionalKVStoreWorkload.multiPutOk;
import static dslabs.kvstore.TransactionalKVStoreWorkload.swap;
import static dslabs.kvstore.TransactionalKVStoreWorkload.swapOk;
import static junit.framework.TestCase.assertFalse;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ShardStorePart4Test extends ShardStoreBaseTest {
    @Test(timeout = 5 * 1000)
    @PrettyTestName("Single group, simple transactional workload - multi server")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test01SingleBasicWithSwap() throws InterruptedException {
        int numGroups = 1, numServersPerGroup = 3, numShardMasters = 3,
                numShards = 2;

        setupStates(numGroups, numServersPerGroup, numShardMasters, numShards);

        runState.start(runSettings);

        joinGroup(1, numServersPerGroup);
        runState.addClientWorker(client(1),
                TransactionalKVStoreWorkload.simpleWorkload);

        runState.waitFor();
        runState.stop();

        runSettings.addInvariant(RESULTS_OK);
    }

    @Test(timeout = 5 * 1000)
    @PrettyTestName("Multi-group, simple transactional workload - multi server")
    @Category(RunTests.class)
    @TestPointValue(10)
    public void test02MultiBasicWithSwap() throws InterruptedException {
        int numGroups = 2, numServersPerGroup = 3, numShardMasters = 3,
                numShards = 2;

        setupStates(numGroups, numServersPerGroup, numShardMasters, numShards);

        runState.start(runSettings);

        joinGroup(1, numServersPerGroup);
        joinGroup(2, numServersPerGroup);
        assertConfigBalanced();

        runState.addClientWorker(client(1),
                TransactionalKVStoreWorkload.simpleWorkload);

        runState.waitFor();
        runState.stop();

        runSettings.addInvariant(RESULTS_OK);
    }

    @Test
    @PrettyTestName("Multi-client, multi-group; MultiPut, Swap, MultiGet")
    @Category(SearchTests.class)
    @TestPointValue(20)
    public void test03MultiClientMultiGroupSearch() {
        setupStates(2, 1, 1, 2);

        Workload w1 = TransactionalKVStoreWorkload.builder().commands(
                multiPut("foo-1", "X", "foo-2", "Y"), swap("foo-1", "foo-2"))
                                                  .results(multiPutOk(),
                                                          swapOk()).build();
        initSearchState.addClientWorker(client(1), w1);

        Workload w2 = TransactionalKVStoreWorkload.builder().commands(
                multiGet("foo-1", "foo-2")).results(
                multiGetResult("foo-1", "Y", "foo-2", "X")).build();
        initSearchState.addClientWorker(client(2), w2);

        multiClientMultiGroupSearch();
    }
}
