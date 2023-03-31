#!/usr/bin/env bash

function verify() {
	arr=("$@")
	for i in "${arr[@]}";
		do
				if [ ! -f $i ]; then

					echo "Missing ${i}"
					exit 1
				fi
		done
}

req_files=("src/dslabs.shardmaster/ShardMaster.java" "src/dslabs.shardkv/Messages.java" "src/dslabs.shardkv/ShardStoreServer.java" "src/dslabs.shardkv/ShardStoreNode.java" "src/dslabs.shardkv/ShardStoreClient.java" "src/dslabs.shardkv/Timers.java" "src/dslabs.kvstore/KVStore.java" "src/dslabs.kvstore/TransactionalKVStore.java" "src/dslabs.atmostonce/AMOCommand.java" "src/dslabs.atmostonce/AMOApplication.java" "src/dslabs.atmostonce/AMOResult.java" "src/dslabs.paxos/PaxosLogSlotStatus.java" "src/dslabs.paxos/Ballot.java" "src/dslabs.paxos/Messages.java" "src/dslabs.paxos/PaxosClient.java" "src/dslabs.paxos/Pvalue.java" "src/dslabs.paxos/PaxosServer.java" "src/dslabs.paxos/PaxosRequest.java" "src/dslabs.paxos/PaxosDecision.java" "src/dslabs.paxos/PaxosReply.java" "src/dslabs.paxos/Timers.java" "REPORT.md")
verify "${req_files[@]}"
if [[ $? -ne 0 ]]; then
    exit 1
fi

if [ $# -eq 1 ]
then
	zip "${1}.zip" src/dslabs.shardmaster/ShardMaster.java src/dslabs.shardkv/Messages.java src/dslabs.shardkv/ShardStoreServer.java src/dslabs.shardkv/ShardStoreNode.java src/dslabs.shardkv/ShardStoreClient.java src/dslabs.shardkv/Timers.java src/dslabs.kvstore/KVStore.java src/dslabs.kvstore/TransactionalKVStore.java src/dslabs.atmostonce/AMOCommand.java src/dslabs.atmostonce/AMOApplication.java src/dslabs.atmostonce/AMOResult.java src/dslabs.paxos/PaxosLogSlotStatus.java src/dslabs.paxos/Ballot.java src/dslabs.paxos/Messages.java src/dslabs.paxos/PaxosClient.java src/dslabs.paxos/Pvalue.java src/dslabs.paxos/PaxosServer.java src/dslabs.paxos/PaxosRequest.java src/dslabs.paxos/PaxosDecision.java src/dslabs.paxos/PaxosReply.java src/dslabs.paxos/Timers.java REPORT.md
else
	echo 'Please provide your GTID, eg ./submit.sh syi73'
fi
