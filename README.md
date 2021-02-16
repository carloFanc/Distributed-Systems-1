# Distributed-Systems-1
This document will present the implementation of a protocol for the coordination of a group of replicas,
sharing the same data and tolerating multiple node failures due to a quorum-based approach. As per the
specification, the system handles read and update requests from external clients. For the read request a
value is provided by the replicas, for the update request it is guaranteed that all replicas eventually apply
the updates in the same order. The updates are handled by a special replica called coordinator that
performs a two phase broadcast procedure.
The system is developed using the Akka framework and is composed of several actors that simulate a
communication over the network. In the first part of the document the architecture of the project will be
described specifying actors and messages used. In the second part of the paper the implementation logic
will be analyzed and then also how the system handles critical events such as node crashes and coordinator
election.
## Client
Client extends the AbstractActor class. The system contains 2 clients and no clients are successively added
to the system. Through the preStart method, random (write or read) requests are scheduled to randomly
chosen replicas.
## Replica
Replica extends the AbstractActor class. The system contains 10 replicas and no replicas are successively
added to the system. Among the replicas, one of them is the coordinator.
Initially the system puts replica number 0 as the coordinator. Each node can crash and then in case the
coordinator crashes, the successor will be elected.

## Read and write operations

The reading operation starts with a client asking a randomly selected replica for its own value. Initially, a
scheduler was implemented in the client's preStart method to start a random write or read request every 2
seconds to simulate a more realistic situation. Afterwards, given the need to manually insert crashes to test
the software, it was decided not to use the scheduler but to make the request manually with the possibility
of inserting the preStart anyway. Regarding the read request, the replica can instantly respond with its local
value.
The write request always starts from the client to a random replica. In this case, the replica will broadcast
the message in such a way that the coordinator is the only one to take charge of that type of message.

## Election phase
The election can start in different ways:
- Expiration of the Heartbeat timeout (starting in onHeartBeatMsg())
- Expiration of the Update timeout after sending an UpdateRequest to the coordinator (starting in
onWriteRequest())
- Expiration of the Write ok timeout after sending the ack to the coordinator (starting in
onUpdateMsg() or in onWriteOk())

On expiration of one of the timeouts, a StartElectionMsg is self-sended, starting the election (and a timer).
Then an ElectionMsg is sent following a ring path using the ids (e.g. replica 3 will send to 4 and the last will
send to 0), crossing all the replicas at least for one ride.
The ElectionMsg contains:
- the History (epoch and sequence number) of the visited replicas (histories)
- the id of the next replica (nextId)
- an election number randomly generated at the start (for debugging purpose)
- a TTL: a number increasing every time the message is received. If it’s bigger than the number of
replicas times 2 (the message did 2 rides), the message is discarded
- a string for debugging purpose (msg)
Everytime a replica receives an ElectionMsg, it computes the winner by choosing the id of the replica with
the most recent Update (epoch and sequence number) and if there are equals, it is chosen the smallest id
(winner id). If the current replica is the winner then it broadcasts a synchronizatonMsg with the last value
and update. Otherwise the current epoch and sequence number is added to the ElectionMsg’s history, the
next id increased and the new ElectionMsg is sent. If the next replica does not respond with an ElectionAck,
the message is sent to the next one. In addition, if the replica isn’t the winner, it goes in electionMode, by
ignoring read and update requests and stopping the heartbeat.
If the election does not end for any reason, it expires the timeout of the replica who started the election
and then it retries a new one.

## Synchronization phase
Once the election phase is over and a new coordinator has been successfully elected, he will broadcast a
Synchronization message. This message will update the current epoch by incrementing it by one so that all
future messages are considered in the new epoch. However, before updating the new epoch, each replica
will check for incomplete updates, resulting from a previous coordinator crash, and send a
CompleteUpdateMsg to the elected coordinator. This message will broadcast a write commit that will
update all replicas with the same value and corresponding epoch.

## Crash simulations
Crashes are handled with the crashMsg: when it is received, a replica sets his next crash with the one in the
message triggering the crash mode on the predetermined position in the code. If it’s a simple crash type,
the replica goes instantly in crash mode, ignoring all messages and canceling the heartbeat.
 
## Concurrent updates
To handle concurrent updates we implemented a vector clock for the UpdateMsg and the Ack (extending
VectorClockMsg): when a replica receives an update message, it checks if his vector clock is concurrent with
with the message and in a positive case it will put the message in a queue.
When a replica receives a write ok, it checks if there are pending updates to complete and it will send the
ack to continue.
