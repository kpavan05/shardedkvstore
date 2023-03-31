# Project 5: FT Sharded KV Store

*Adapted from the [UW CSE452 Labs](https://github.com/emichael/dslabs/tree/master/labs/lab4-shardedstore)*

In this project, you'll build a linearizable key/value storage system that "shards"
(partitions) the keys over a set of replica groups and handle cross-group
transactions with the two-phase commit. In this project, a shard is a subset of the 
key/value pairs; for example, all the keys starting with "a" might be one shard, 
all the keys starting with "b" another, etc. The reason for sharding is performance. 
Each replica group handles puts and gets for just a few of the shards, and the groups
operate in parallel; thus total system throughput (puts and gets per unit time)
increases in proportion to the number of groups.

Your sharded key/value store will have two main components. First, a set of
replica groups. Each replica group is responsible for a subset of the shards; a
replica group consists of a set PAXOS servers. The second component is the
*Shard Master*. The *Shard Master* decides which replica group should serve each
shard; this information is called the configuration. The configuration changes
over time. Clients consult the *Shard Master* in order to find the replica group
for a key, and replica groups consult the master in order to find out what
shards to serve. There is a single *Shard Master* group for the whole system,
implemented as a fault-tolerant service using PAXOS.

A sharded storage system must be able to shift shards among replica groups. One reason 
is that some groups may become more loaded than others, so that shards need to be moved 
to balance the load. Another reason is that replica groups may join and leave the 
system: new replica groups may be added to increase capacity, or existing replica 
groups may be taken offline for repair or retirement. We will simulate these situations 
by issuing commands to *Shard Master* to update shard configurations from an outsider 
for testing. The architecture of this project is shown as follows.

<img src="/lab/pic/p5arch.png" width="1600"/>

We have provided you a sketch of the overall design in source files. The tests for this project are even more demanding. You have to think hard to design and finish your implementation in a flawless way. Along with the implementation, we will guide you with some illustrations about several tests to help you understand the test logic. We will use the message interfaces defined in Project 4 (`PaxosRequest`, `PaxosDecision`, and `PaxosReply`). `PaxosRequest` and `PaxosDecision` are for exchanging internal messages between `ShardStoreServer` and underlying `PaxosServer`, while `PaxosReply` is the reply containing the decided `ShardConfig` from `ShardMaster`. You might need to read those helping docs in Project 4 source (`PaxosRequest.java`).

Part 1 *Shard Master* and part 2 *Sharded KV Server* (single-server) are easier, while part 3 *Sharded KV Server* (multi-server), part 4 (single-server & multi-server), and part 5 *Transactional KV Store* are more challenging. Your implemented PAXOS will be used to provide fault-tolerance and the commands ordering for system state updates, i.e. client requests, internal state changes, etc. Part 1 *Shard Master* doesn't rely on your PAXOS, and it will only test the strategy correctness for shards relocation when there are server groups added or removed. The left parts will need PAXOS for fault-tolerance and command ordering. For part 2 and part 4, we have added some dummy tests with only 1 server in each server group (no fault-tolerance, but still use PAXOS). You should pass them to gain some confidence with your system before proceeding to those multi-server ones. Passing those tests doesn't provide any correctness guarantee for multi-server running. The rest tests (part 3, part 4, and part 5) will employ multiple servers in each server group, for both *Shard Master* and your KV application. For those tests, you need to ensure your PAXOS can pass tests in Project 4 reliably at most time, otherwise, you might need to work on Project 4 to refine your implementation. With incorrect PAXOS, it would be difficult to identify the problems when tests fail. But it never hurts to try :).

We have separated the three operators implemented for *Transactional KV Store* into two parts, i.e. part 4 and part 5. Part 4 will merely use `MultiGet` and `MultiPut`, while `Swap` is tested in part 5. You can pass the starting 4 parts without implementing the `Swap` operator. **The part 5 implementation is for students who want to try for extra credit, taking up to an extra 5% of the overall grades for Project 5. You can at most get 95% for the project implementation**.

There will be two submission options for Project 5. The first one is to use your implementation of PAXOS for service. And the second will use our reference PAXOS implementation (you can't see the actual implementation, but you can submit it to GradeScope to check results). It will run your Project 5 with our reference PAXOS. This would help you check the correctness of Project 5 and exclude the influence of PAXOS, especially for multi-server settings. We will illustrate this in detail in the following sections.

A test version of single-server PAXOS, a basic client-server implementation, is currently under `lab/src` for you to get a sense of our reference PAXOS (a multi-server one) in GradeScope. That version mimics the structure of our reference solution. You can refer to [Reference PAXOS](#reference-paxos) for more details. **Even with the provided single-server PAXOS, you can almost get over 80% for correct implementations of Project 5, excluding the report. The test suites for Project 5 put more emphasis on the reconfigurations and two-phase commits.**

Before you start, please make sure your implementation for the exactly-one application and KV store is correct (`project2: dslabs.kvstore` and `project2: dslabs.atmostonce`). As for PAXOS, you can use the provided single-server one or your implementation for P4. A workable version with 1 server as a group is the minimum requirement for starting Project 5. Your implementation for Project 5 will be built upon the Project 2 and Project 4 source. **You should copy your implementation of Project 2 and Project 4 to the corresponding position of Project 5 src. If you want to use the provided single-server PAXOS, you can leave** `dslabs.paxos` **untouched.** There will be some modifications in `PAXOS` and we will illustrate them below. Also, we suggest you read through the questions from previous projects, especially Project 1 and 2. There are some questions applied to all projects. You should also refresh yourself with the assumptions that we summarized in Project 4 doc. You might find those questions and assumptions helpful.

1. [Project Setup](#project-setup)
2. [Introduction](#introduction)
3. [Part 1: Shard Master](#part-1-shard-master)
4. [Part 2 & 3: Sharded KV Server and Reconfiguration](#part-2--3-sharded-kv-server-and-reconfiguration)
5. [Part 4 & Part 5: Transaction](#part-4--4-transaction)
6. [Visual Debugging](#visual-debugging)
7. [Reference PAXOS](#reference-paxos)
8. [Submission](#submission)
9. [Project Question List](#project-question-list)
10. [Recommend Reading List](#recommend-reading-list)

## Project Setup
We assume you have already configured your virtual machine with Ubuntu 18.04, downloaded Java 8, installed git, and have an IDE for your implementation. 
If not, please refer to the `environment` repo in our organization.

You will first clone the project repo from our organization. Please use the following command for cloning from our organization repo. 
You should replace `gtid` with your GTID, `course-organization` to the current term organization, and `project-repo` to the project repo name. You will be asked to enter a password for access. The password is the same as your GT account.

```shell script
git clone https://<gtid>@github.gatech.edu/<course-organization>/<project-repo.git>
```

The project repos are private and we only grant access to enrolled students. If you have enrolled but cannot clone project repos (in general, if you can see this repo, you should be able to clone it), please contact the course TAs for addressing the issues. We only allow read and clone permissions for students without push. Please initial your repo inside your GitHub account and update this repo to it for further usage. We have provided detailed commands to clone and create copy inside your private Gatech GitHub in the `environment` repo under the same organization.

After cloning the project repo, open the project directory with IDE (IntelliJ). Also, open a terminal and enter the project directory. Enter ``make`` for the crash will automatically download all dependencies and you should no longer see import reference issues in IDE. In case of build failure shows as below, `sudo chmod +x gradlew` should fix your problem.

<div style="text-align:center;"><img src="/lab/pic/makeerror.png"/></div>

## Introduction
Part 1 *Shard Master* is much easier than the rest parts. You just need 
to think carefully about some edge cases and try to even the shards in current 
replica groups.

The main challenge in parts 2 and 3 of this project will be handling reconfiguration 
in the replica groups. Within a single replica group, all group members must agree 
on when a reconfiguration occurs relative to the client `Put`/`Append`/`Get` requests. 
For example, a `Put` may arrive at about the same time as a reconfiguration that 
causes the replica group to stop being responsible for the shard holding the `Put`'s 
key. All replicas in the group must agree on whether the `Put` occurred before or 
after the reconfiguration. If before, the `Put` should take effect and the new owner 
of the shard will see its effect; if after, the `Put` won't take effect and client 
must re-try at the new owner. The recommended approach is to have each replica group 
use PAXOS to log not just the sequence of `Put`s, `Append`s, and `Get`s but also the 
sequence of reconfigurations and send these decisions in sequence for `ShardStoreServer` 
acting according to them.

The reconfiguration also requires interaction among the replica groups. For example,
in configuration 10 group G1 may be responsible for shard S1. In configuration
11, group G2 may be responsible for shard S1. During the reconfiguration from 10
to 11, G1 must send the contents of shard S1 (the key/value pairs) to G2.

You will need to ensure that at most one replica group is serving requests for
each shard. Luckily it is reasonable to assume that each replica group is always
available because each group uses PAXOS for replication and thus can tolerate
some network and server failures. As a result, your design can rely on one group
to actively hand off responsibility to another group during reconfiguration.
This is simpler than the situation in primary/backup replication, where the old
the primary is often not reachable and may still think it is primary.

In part 4 and part 5 you will extend your key-value store to handle multi-key transactions.
When these transactions touch shards held by different replica groups, you will
use two-phase commit with locking to ensure linearizability of operations.

This project's general architecture (a configuration service and a set of replica
groups) is patterned at a high level on a number of systems: Flat Datacenter
Storage, BigTable, Spanner, FAWN, Apache HBase, Rosebud, and many others. These
systems differ in many details from this projects, though, and are also typically
more sophisticated and capable. For example, your project lacks persistent storage
for key/value pairs and for the PAXOS log; it cannot evolve the sets of peers in
each Paxos group; its data and query models are very simple, without SQL supports; 
and handoff of shards is slow and doesn't allow concurrent client access.

## Part 1: Shard Master
The `ShardMaster` manages a sequence of numbered configurations (starting with
`INITIAL_CONFIG_NUM`). Each configuration describes a set of replica groups and
an assignment of shards (numbered 1 to `numShards`) to replica groups. Whenever
this assignment needs to change, the shard master creates a new configuration
with the new assignment. Key/value clients and servers contact the `ShardMaster`
when they want to know the current (or a past) configuration.

The `ShardMaster` runs as an `Application` and will be replicated with your
implementation of Paxos, which means that as long as it is deterministic, it
will be fault-tolerant and guarantee linearizability and exactly-once semantics
for operations.

Your implementation must support the `Join`, `Leave`, `Move`, and `Query`
operations in `ShardMaster`.

`Join` contains a unique positive integer replica group identifier (`groupId`)
and set of server addresses. The `ShardMaster` should react by creating a new
configuration that includes the new replica group. The new configuration should
divide the shards as evenly as possible among the groups and should move as few
shards as possible to achieve that goal. The `ShardMaster` should return `OK`
upon successful completion of a `Join` and `Error` if that group already exists
in the latest configuration.

`Leave` contains the `groupId` of a previously joined group. The `ShardMaster`
should create a new configuration that does not include the group, and that
assigns the group's shards to the remaining groups. The new configuration should
divide the shards as evenly as possible among the groups and should move as few
shards as possible to achieve that goal. The `ShardMaster` should return `OK`
upon successful completion of a `Leave` and `Error` if that group did not exist
in the latest configuration.

`Move` contains a shard number and a `groupId`. The `ShardMaster` should create
a new configuration in which the shard is assigned to the group and only that
shard is moved from the previous configuration. The main purpose of `Move` is to
allow us to test your software, but it might also be useful to load balance if
some shards are more popular than others or some replica groups are slower than
others. A `Join` or `Leave` following a `Move` could undo a `Move`, since `Join`
and `Leave` re-balance. The `ShardMaster` should return `OK` upon successful
completion of a `Move` (one which actually moved the shard) and `Error`
otherwise (e.g., if the shard was already assigned to the group).

`Query` is a read-only command and it contains configuration number. The `ShardMaster` 
replies with a `ShardConfig` object that has that configuration number. If the number 
is -1 or larger than the largest known configuration number, the `ShardMaster` should
reply with the latest configuration. The result of `Query(-1)` should reflect
every `Join`, `Leave`, or `Move` that was completed before the `Query(-1)` was sent.

The very first configuration, created when the first `Join` is successfully
executed, should be numbered `INITIAL_CONFIG_NUM`. Before this configuration is
created, the result of a `Query` should be `Error` instead of a `ShardConfig`
object.

You will use `new MutablePair<>(.., ..)` to instantiate pair for configuration.

Our solution to part 1 took approximately 200 lines of code.

You should pass the part 1 tests before moving on to part 2 and part 3; execute them with
`python3 run-tests.py --lab 5 --part 1`.

## Part 2 & 3: Sharded KV Server and Reconfiguration
Now you'll build a sharded fault-tolerant key/value storage system. Part 2 
will test your implementation with one server in each group, and Part 3 will 
test the multi-server setting.

Each `ShardStoreServer` will operate as part of a replica group. Each replica
group will serve operations for some of the key-space shards. Use `keyToShard()`
in `ShardStoreNode` to find which shard a key belongs to; you should use use
`SingleKeyCommand.key()` (all of the operations you'll handle in part 2 and part 3 
are single-key operations) in `ShardStoreClient` to determine the key for a given
operation.

Multiple replica groups will cooperate to serve the complete set of shards. A
replicated `ShardMaster` service will assign shards to replica groups. When this
assignment changes, replica groups will have to hand off shards to each other.
Your storage system must provide linearizability of `KVStore` operations passed
to `ShardStoreClient`. This will get tricky when `Get`s, `Put`s, and `Append`s
arrive at about the same time as configuration changes.

You are allowed to assume that a majority of servers in each replica group are
alive and can talk to each other, can talk to a majority of the `ShardMaster`
servers, and can talk to a majority of every other replica group. Your
implementation must operate (serve requests and be able to re-configure as
needed) if a minority of servers in some replica group(s) are dead, temporarily
unavailable, or slow.

Your servers should not try to send `Join` operations to the `ShardMaster`. The
tests will send configuration changes when appropriate. `ShardStoreServer` and
`ShardStoreClient` should only send `Query`s to the `ShardMaster` servers.

Your `ShardStoreServer` should use PAXOS to replicate operations among replicas
in the same replica group as follows: 

First, modify `PaxosServer` by adding another constructor, which, instead of 
taking an application takes an `Address` and there will be no application inside 
this PAXOS. When started this way, your `PaxosServer` should run exactly the same 
as before, except instead of executing commands, it sends all decisions in order to 
the given address (using `handleMessage` described below).

```java
// for sub node constructor
private final Address rootAddress;
// for application constructor
private final AMOApplication<Application> app;
...

public PaxosServer(Address address, Address[] servers, Address rootAddress) {
    super(address);
    this.servers = servers;

    this.app = null;
    this.rootAddress = rootAddress;
}
...
```

Next, in `ShardStoreServer`, create a `PaxosServer` and initialize it as below.

```java
private static final String PAXOS_ADDRESS_ID = "paxos";
private Address paxosAddress;

public void init() {
    // Setup Paxos
    paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

    Address[] paxosAddresses = new Address[group.length];
    for (int i = 0; i < paxosAddresses.length; i++) {
      paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
    }

    PaxosServer paxosServer =
        new PaxosServer(paxosAddress, paxosAddresses, address());
    addSubNode(paxosServer);
    paxosServer.init();

    ...
}
```

This sets up a PAXOS group for each replica group. `ShardStoreServer`s can then
pass operations to their local PAXOS node to be proposed by calling
`handleMessage(message, paxosAddress)`. Nodes within the same root node can pass
messages to each other through this interface. Thus, you will also need to update 
your PAXOS to allow it sending `PaxosDecision` back to the `ShardStoreServer` 
using the same method. These messages are not sent over the network but are immediately (and reliably) handled.

Your `ShardStoreServer`s should instantiate their own local `KVStore` (wrapped
in an `AMOApplication`). Unlike previous systems, this system will not be able
to handle different underlying `Application`s.

Finally, your `ShardStoreServer` and `ShardStoreClient` should periodically send
`Query` operations to the `ShardMaster`s to learn about new configurations. In
order to do this without creating sequence numbers for each `Query` sent, you
might want to modify `PaxosServer` to allow it to handle read-only non-AMO
commands, and send `Query`s (which are read-only) as simple `Command`s.

To summary, your `ShardStoreServer` will receive:
1. `PaxosReply` from `ShardMaster`, informing the servers about new configurations.
2. `ShardStoreRequest` from `ShardStoreClient` for the KV operation requests.
3. Other messages from servers in other groups, such as shard transfer messages.
4. Local PAXOS decisions `PaxosDecision` from sub-nodes.

We recommend you to follow one uniform workflow for all received messages from servers in other groups and *Shard Master*. Once receiving new messages, do not perform 
any action but just sending it to PAXOS and waiting for the ordered decisions.
1. Perform certain validation, e.g. checking whether the configuration match, etc.
2. Once verified, propose command, generated local `id` and `sequenceNum` by sending `PaxosRequest` to local PAXOS
3. After receiving decisions from PAXOS, perform actions according to the decisions. All servers in one group will see the decisions in the same order (PAXOS log). We recommend you to add some intermediate methods with an extra parameter indicating whether the command is decided in PAXOS or not. We show an example as follows.

```java
private void handleShardStoreRequest(Request m) {
    // perform validation -> possible pre-pocessing
    process(command, false);
}

private void process(Command command, boolean replicated) {
    if (command instanceof ShardMove) {
        processShardMove((ShardMove) command, replicated);
    } else if (command instanceof ShardMoveAck) {
        processShardMoveAck((ShardMoveAck) command, replicated);
    } else if (command instanceof NewConfig) {
        processNewConfig((NewConfig) command, replicated);
    } else if (command instanceof AMOCommand) {
        processAMOCommand(command, replicated);
    }
    ...
}

private void processAMOCommand(AMOCommand command, boolean replicated) {
    // generate local proposing `PaxosRequest`
    if (!replicated) {
        this.handleMessage(new PaxosRequest(...), paxosAddress);
        return;	
    }
    ...
}

private handlePaxosDecision(PaxosDecision m, Address sender) {
    process(m.command(), true);
}
```

---
Our solution to part 2 and part 3 took approximately 400 lines of code.

You should pass the part 2 and part 3 tests; execute them with `python3 run-tests.py --lab 5 --part
2` and `python3 run-tests.py --lab 5 --part 3`.

### Hints
- You should handle all communication between replicas in the same group through
  PAXOS. Replicas should propose operations to the PAXOS log, and they will all
  process them in the same order. This should include key-value operations and
  also any operations needed for reconfiguration. You should create your own
  sub-Interface of `Command` which all of your reconfiguration-specific
  operations inherit from so that you can easily propose these operations to the
  PAXOS log. We recommend you to follow the workflow illustrated before.
- The easiest way for a replica/group to send a message to a different group is
  by broadcasting the message to the entire group.
- Your server should respond with an error message to a client operation on a
  key that the server isn't responsible for (i.e. for a key whose shard is not
  assigned to the server's group). As in the primary-backup case, the client can
  then go back to the `ShardMaster`s to learn about the latest configuration.
- Process and request re-configurations one at a time, **in order**.
- During re-configuration, replica groups will have to send each other the keys
  and values for some shards (you can either store each shard in a separate `KVStore`, 
  or you might have to modify `KVStore` to support some different ways). 
- Be careful about guaranteeing at-most-once semantics for key-value operations.
  When a server sends shards to another, the server needs to send
  `AMOApplication` state as well. Think about how the receiver of the shards
  should update its `AMOApplication` state. If you choose to store each shard in a 
  different `KVStore`, you will need to transfer that `KVStore` to other groups 
  (like primary/backup) during reconfiguration, and remember to make a deep copy.
- Think about when it is okay for a server to give shards to the other server
  during re-configuration.
- The majority of the search tests for this project assume that your PAXOS
  implementation is correct and only model-check the new protocol. They do this
  by instantiating all PAXOS groups with a single server (part 2). Your PAXOS
  implementation should be able to reach an agreement in a single step when there
  is only one server.

## Part 4 & 5: Transaction
Finally, you'll extend your key-value store to support cross-group transactions
using two-phase commit.

First, you'll need to complete the `execute` method in `TransactionalKVStore` to
extend your key-value store to support the `Transaction` interface we define. 
There are three operators, i.e. `MultiGet`, `MultiPut`, and `Swap`. This should 
take one or just a few lines of code. You should then initialize your `ShardStoreServer` 
with a `TransactionalKVStore` rather than a `KVStore`.

With those preliminaries out of the way, you'll then need to modify your server
to handle transactions, as well as your client to route transactions to the
correct server. You'll use two-phase commit with locking to ensure
serializability of transactions. There are two roles for servers participating 
in two-phase commit: coordinator and participant. The coordinator will monitor 
the progress of transactions and determine whether to abort current transactions. 
In the first phase, one group, serving as transaction coordinator, will send a 
prepare request; the other participants in the transaction, upon receiving the 
prepare request, will acquire read and write locks for the transaction and 
respond. If all groups participating in the transaction respond and no group 
aborts, the transaction coordinator will send the commit message to all groups; 
the groups will then free the locks and acknowledge the commit message. As 
previously stated, you are free to assume that a majority of servers from each 
group will remain active. Furthermore, if any nodes do fail, they fail by 
crashing. You *do not* need to implement a node failure recovery protocol.

Your system should guarantee linearizability of all transactions and be
deadlock-free; it should never reach a state where it cannot make progress.
Furthermore, it should always be able to process reconfigurations, and when
there are no ongoing reconfigurations are no conflicting transactions, it should
be able to make progress and commit transactions (as long as the consensus
protocol underlying each group continues to make progress, of course). You do
not need to guarantee fairness, however (more on this below).

You should think carefully about how transactions will interact and interleave
with reconfigurations. Unless handled with extreme care, attempting to execute
transactions with concurrent reconfigurations could lead to deadlock or
linearizability violations. Therefore, we suggest the following approach:
- All `ShardStoreServer` nodes tag their transaction-handling messages with
  their configuration number.
- Servers reject any prepare requests coming from different configurations,
  causing the transaction to abort.
- Servers *delay* reconfigurations when there are outstanding locks for keys
  (i.e., there are transactions pending in the previous configuration).

This means that any transaction will occur entirely in one configuration or
another. Even given that, however, you'll have to be careful to avoid deadlock.
The easiest way to avoid deadlock is to have your servers *reject any prepare
requests when they cannot acquire locks* and cause the transaction to abort.
This could lead to livelock if concurrent transactions continually cause each
other to abort. You can lessen the chance that this will happen, however, by
giving each group a fixed priority (for instance, using its group ID) and having
the clients always send their transactions to the group with highest priority
among transaction participants (and enforcing that choice server-side). While
eliminating transaction livelock and guaranteeing fairness among transactions
are not requirements for this project, they are important properties in practice,
and you should think about how you would go about achieving them!

---
Our solution to part 4 and part 5 took approximately 400 lines of code.

You should pass the part 4 tests; execute them with `run-tests.py --lab 5 --part
4`. For part 5 extra credits, execute them with `run-tests.py --lab 5 --part
5`.

### Hints
* When a transaction gets aborted, it will need to be retried. You will need to
  be able to differentiate the prepare responses/aborts across different
  attempts to commit the same instance of a transaction.
* While you should assume that the test code always sends commands one-at-a-time
  (i.e., waits for a result before sending the next command), you may (or may
  not) have to go to greater lengths in this project to ensure that transactions
  from the same client get processed in the same order on all replicas.

## Visual Debugging
The arguments to start the visual debugger for this project are slightly different.
To start the visual debugger, execute `python3 run-tests.py -d NUM_GROUPS
NUM_SERVERS_PER_GROUP NUM_SHARDMASTERS NUM_CLIENTS CLIENT_WORKLOAD
[CONFIG_WORKLOAD]` where:
- `NUM_GROUPS` is the number of `ShardStoreServer` groups
- `NUM_SERVERS_PER_GROUP` is the number of `ShardStoreServer`s in each group
- `NUM_SHARDMASTERS` is the number of Paxos servers replicating the
  `ShardMaster`
- `NUM_CLIENTS` is the number of `ShardStoreClient`s
- `CLIENT_WORKLOAD` is the `ShardStoreClient`'s workload, a comma-separated list
  of `KVStoreCommand`s. These can include `GET`, `PUT`, and `APPEND`, which have
  the normal syntax, as well as transactions, which have the following syntax:
  - `MULTIGET:key1:key2:...:keyN`
  - `MULTIPUT:key1:value1:key2:value2:...:keyN:valueN`
  - `SWAP:key1:key2`
- `CONFIG_WORKLOAD` is an optional comma-separated list of commands for the
  `ShardMaster`s of the form `JOIN:groupId`, `LEAVE:groupId`, or
  `MOVE:groupId:shardNum`. The default is one `Join` for each group.

The default number of shards is 10, though this can be changed in
`ShardStoreVizConfig`.

## Reference PAXOS
To prevent students from being stuck on an imprecise PAXOS implementation, we have provided an extra submission option in GradeScope, which uses our reference solution for PAXOS to provide the FT and message ordering service. You will not have access to the actual code but can only submit your Project 5 to the corresponding assignment in GradeScope to see the test results. We now illustrate the high-level implementation of our PAXOS and provide you with some helping notes for using the reference implementation.

Our reference solution contains all source files as `dslabs.paxos` and the at-most-once application with commands (this is for *Shard Master*). You will not need to worry about the conflicts between our PAXOS solution and your implementation. Although the `AMOApplication` and corresponding commands are used in `PAXOS`, we have relocated our `AMOApplication` and commands to `dslabs.paxos`, and made use of them in our PAXOS. Thus, you should be safe to continue to use your `AMOApplication` and commands in your `ShardStoreServer` without being affected by our reference solution. The autograder will replace your PAXOS directory (the application & commands that are inside the PAXOS directory) when running in GradeScope.

Your `ShardStoreClient` and `ShardStoreServer` should propose `PaxosRequest` to `ShardMaster` with command as `Query`. A `PaxosReply` from `ShardMaster` with result as `ShardMasterResult` (`ShardConfig` or `Error`) will be sent back. The `id` and `sequenceNum` of `PaxosRequest` and `PaxosReply` are the same.

You will use `PaxosRequest` to post commands to PAXOS for ordering. PAXOS will send back the decided slot as a `PaxosDecision` in the order of PAXOS log. Our PAXOS follows the `in-order request generation` assumption (see Project 4 doc), and we have added duplication detection mechanisms in PAXOS. This means that your `ShardStoreServer` should not post `PaxosRequest`s with the same `id` and a higher sequence number before the previous request has been handled by PAXOS. If you propose a command with a higher sequence number and receive decisions for the same, you will not hear the decisions of previous requests, even if you post them again.

You should think carefully about the `id` and `sequenceNum` to use for all `PaxosRequest`s being posted to PAXOS, including client requests, shard config change requests, and transaction requests. Incorrect usage might trigger dead-lock problems, especially for transaction commands. You might want to use the configuration number as a part of `id`. You could use the configuration number as `sequenceNum` for a new replicated configuration, and you don't need to strictly increment `sequenceNum` by 1 each time. Please remember that **your `ShardStoreServer` can receive the PAXOS decisions reliably and received decisions are exactly in the order of the PAXOS log (one-by-one)**, and your implementation for `ShardStoreServer` should act as a state machine. Please do follow the message interfaces defined in PAXOS source, if you want to use our reference implementation.

We have included a test version of single-server PAXOS for you to get a sense of the fully functional PAXOS (a multi-server one) we provided in GradeScope. The source structure of that test version mimics the one in our reference solution. The logic for processing incoming `PaxosRequest` is shown in the diagram below. You can also refer to the code and check the simplified logic.

<img src="/lab/pic/Paxos.png" width="1600"/>

## Submission
Project 5 requires a fair amount of code along with huge efforts for a correct implementation. It is the most difficult one. 

As we said, we have provided two programming assignments for submission in GradeScope. Both of them will run all test cases for Sharded KV Store. The one denoted with `reference` will use our reference PAXOS implementation, and your Sharded KV Store and *Shard Master* will use it for the FT and message ordering. The other submission will use all your implementation for running tests. We use one zip file to test your implementation in GradeScope. The autograder will replace your PAXOS with our reference one if needed. You should also write a simple report. We have provided you the general structure in `REPORT.md`.

If you are using our provided single-server PAXOS (not replace the `dslabs.paxos` with your implementation for Project 4) and you want to use our multi-server PAXOS, please submit to the `reference` one, since the source file structure is not the same as Project 4. 

Also, if you just want to submit with a single-server PAXOS, you can use the provided one. But remember to change the AMO parts to your implementation for Project 2 in `dslabs.paxos/PaxosServer.java::sendRequestReply` and `dslabs.paxos/PaxosClient.java` as well as the corresponding package dependencies. You should submit to the `non-reference` one. As for your own implementation (with the same source file structure as in Project 4), you can submit to both options.

For submission, you should submit both your implementation and report. As for the report, fill the content in  `REPORT.md`. A `submit.sh` under `lab` is ready to use. Run that script as follows and enter your GTid. A zip file with the same name as your GTid, `GTid.zip`, will be generated. The zip file should contain your implementation source code and `REPORT.md`. Submit the `zip` file to the corresponding project in GradeScope. Then, you are done! You can choose to submit one or both programming assignments in GradeScope. We will use your last submission and take the higher score from those assignments for project grading, and we reserve the right to re-run the autograder. The running setting of autograder in GradeScope is 4 CPUs with 6G RAM.

**WE DO ENCOURAGE YOU TO USE YOUR OWN PAXOS AND IT IS FUN!**

```shell script
$ submit.sh GTid
```

***Note**: Make sure you do not include **print** or **log** statements in your implementation. Also, do not include or zip extra files for your submissions. We will check the completeness and validity of submission before grading. 
If your submission fails to satisfy the submission requirement or could not compile, you will see feedback from GradeScope indicating that and receive 0 for that submission.*

***
### Submission Metrics
- `GTid.zip` (Implementation Correctness 90%, Extra Credits [Part 5] 5%, Report 10%)

## Project Question List

***

- It is stated that the `ShardMaster` should distribute the shards as even as possible. Does this mean we need some hash function like the one in consistency hash for load balancing?

	No. You don't need to implement or use any hash function to evenly distribute the shards. Some simple approximation would work well.

## Recommend Reading List

- [Google Spanner](https://static.googleusercontent.com/media/research.google.com/zh-CN//archive/spanner-osdi2012.pdf)
- [Google Spanner Product](https://cloud.google.com/spanner)
- [DynamoDB](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Retrospective DynamoDB](https://www.allthingsdistributed.com/2012/01/amazon-dynamodb.html)
- [Strong Consistency](https://www.allthingsdistributed.com/2012/01/amazon-dynamodb.html)


***

## Concluding Remarks
:tada::tada::tada: Congrats to those warriors who made it through this semester! :tada::tada::tada:

It is quite a journey ~ We believe all of you have learned something in system programming from these projects and enjoy a good time with them :ok_hand:. Although there is still a large gap from those industrial transactional fault-tolerance applications (especially in testing):unamused::weary::sob:, you will have the confidence for the greater challenges in the future :muscle:. Wish you all the best!