package dslabs.paxos;

import dslabs.framework.Message;
import dslabs.framework.Command;
import lombok.Data;

/**
 * - id:
 *     a unique identifier for PAXOS to identify the sender. It can
 *     optionally also be used to differentiate between the
 *     different types of messages
 * - sequenceNum:
 *     sequence number for the messages with the same id
 * - command:
 *     actual command (AMOCommand, state transfer commands for Project 5)
 *
 * Notification about Project 5:
 * There will be two submission options for Project 5, the Shard KV Store.
 *  1) Use your implementation of PAXOS.
 *  2) Use our reference PAXOS implementation (you can't see the actual
 *  implementation, but can only submit to GradeScope to check results).
 *
 * {@link PaxosRequest}, {@link PaxosReply}, and {@link PaxosDecision}
 * are used in our reference PAXOS service for communication between
 * your `ShardStoreServer` and PAXOS sub-nodes.
 *
 * You should propose to PAXOS sub-node using {@link PaxosRequest}, and
 * receive ordered decisions as {@link PaxosDecision}. The fields in
 * them are the same. {@link PaxosReply} is the response sent back from
 * the ShardMasters, a service you will implement to control the shards
 * in different server groups. We will illustrate the details in Project
 * 5 doc. Please do not alter the classes mentioned if you wish to use
 * our reference implementation for Project 5.
 *
 * You will only use {@link PaxosRequest} and {@link PaxosReply} for Project 4.
 * For detecting duplicates, once a message with a higher 'sequenceNum' is
 * recorded, PAXOS can ignore the smaller 'sequenceNum's belonging to the same 'id'.
 *
 * Useful Note:
 * When designing 'id', please keep the "In-order request generation" assumption
 * in minds, i.e. no new request with a higher 'sequenceNum' will be proposed
 * until the previous requests of the same 'id' are serviced completely.
 *
 * For example:
 *  1) Client can use its address as `id` and `sequenceNum` for
 *     different requests. This would be enough for Project 4.
 *
 *  2) Messages between different server groups can declare `id`
 *     as a combination of the group id, command type, and some
 *     variables indicating current group states (e.g. configuration
 *     number in Project 5).
 *
 *     PaxosRequest 1
 *          ('id': "Group1-shardMove", 'sequenceNum': 14 (Configuration Number), ...)
 *     PaxosRequest 2
 *          ('id': "Group1-shardMove", 'sequenceNum': 15 (Configuration Number), ...)
 *
 *     ** 2 should not be proposed to PAXOS, until the proper response of
 *     1 is received.**
 *
 *     Recalls that we expect all servers in one group acting
 *     as one server, at seen by an outsider. Thus, for the
 *     same command from servers in a group, both `id` and `sequenceNum`
 *     should be the same and there must be no divergence between different
 *     servers. This would be useful for Project 5.
 *
 */

@Data
public final class PaxosRequest implements Message {
    private final String id;
    private final int sequenceNum;
    private final Command command;
}
