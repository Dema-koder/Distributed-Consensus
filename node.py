import argparse
import random
import threading
import time
import concurrent.futures
from concurrent import futures

import grpc

import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc

def get_stub(channel):
    channel = grpc.insecure_channel(channel)
    return pb2_grpc.RaftNodeStub(channel)

NODE_ID = None
SERVERS_INFO = {}
SUSPEND = None

class Node:
    def __init__(self):
        self.id = NODE_ID
        self.state = "Follower"
        self.term = 0
        self.votedFor = None
        self.suspended = False
        self.uncommited_value = 0
        self.committed_value = 0
        self.timer = 0
        self.election_timeout = random.randint(2000, 4000)
        self.append_entries_timeout = 400
        self.leaderId = -1
        print('STATE: Follower | TERM: 0')

    def send_request_vote(self, server):
        try:
            stub = get_stub(SERVERS_INFO[server])
            response = stub.RequestVote(pb2.RequestVoteArgs(candidate_id=self.id, candidate_term=self.term))
            return response
        except grpc.RpcError:
            pass

    def makeRequestVote(self):
        num_vote = 1

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for server in SERVERS_INFO:
                if server == self.id:
                    continue
                futures.append(executor.submit(self.send_request_vote, server))

            for future in concurrent.futures.as_completed(futures):
                try:
                    response = future.result()   
                    if response != None:
                        num_vote += (response.vote_result == True)
                        if response.term > self.term:
                            self.becomeFollower(response.term, None)
                            return
                except grpc.RpcError:
                    continue

        print(f'Votes aggregated {num_vote}')
        if num_vote >= len(SERVERS_INFO) / 2 + 1:
            self.becomeLeader()


    def becomeCandidate(self):
        self.term += 1
        self.state = "Candidate"
        self.votedFor = self.id
        self.timer = 0
        print(f'STATE: Candidate | TERM: {self.term}')
        self.makeRequestVote()

    def becomeFollower(self, term, votedFor):
        self.term = term
        self.votedFor = votedFor
        self.state = "Follower"

    def becomeLeader(self):
        self.leaderId = self.id
        self.state = "Leader"
        print(f'STATE: Leader | TERM: {self.term}')

    def handleFollower(self):
        if self.timer >= self.election_timeout:
            print(f'TIMEOUT Expired | Leader Died')
            self.becomeCandidate()

    def handleCandidate(self):
        if node.timer >= node.election_timeout:
            node.election_timeout = random.randint(2000, 4000)
            node.becomeFollower(self.term, None)

    def handleLeader(self):
        if self.timer % self.append_entries_timeout == 0:
            self.timer = 0
            num_vote = 1

            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = []
                for server in SERVERS_INFO:
                    if server == self.id:
                        continue
                    futures.append(executor.submit(self.send_append_entries, server))

                for future in concurrent.futures.as_completed(futures):
                    try:
                        response = future.result()
                        if response != None:
                            num_vote += (response.heartbeat_result == True)
                            if response.term > self.term:
                                self.becomeFollower(response.term, -1)
                                return
                    except grpc.RpcError:
                        pass
            if num_vote >= len(SERVERS_INFO) / 2 + 1:
                self.committed_value = self.uncommited_value

    def send_append_entries(self, server):
        try:
            stub = get_stub(SERVERS_INFO[server])
            response = stub.AppendEntries(pb2.AppendEntriesArgs(leader_id=self.id, leader_term=self.term, 
                                                                committed_value=self.committed_value, uncommitted_value=self.committed_value))
            return response
        except grpc.RpcError:
            pass

    def handleCurrentState(self):
        if self.suspended == False:
            if self.state == 'Follower':
                self.handleFollower()
            elif self.state == 'Candidate':
                self.handleCandidate()
            else:
                self.handleLeader()


node = None

class Handler(pb2_grpc.RaftNodeServicer):
    def __init__(self):
        super().__init__()

    def AppendEntries(self, request, context):
        global SUSPEND
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.AppendEntriesResponse()
        
        term = request.leader_term
        if node.term > term:
            return pb2.AppendEntriesResponse(term=node.term, heartbeat_result=False)
        if term > node.term:
            node.becomeFollower(term, request.leader_id)
        node.timer = 0
        node.leaderId = request.leader_id
        node.term = term
        node.committed_value = request.committed_value
        node.uncommited_value = request.uncommitted_value

        return pb2.AppendEntriesResponse(**{"term": node.term, "heartbeat_result": True})

    def RequestVote(self, request, context):
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.RequestVoteResponse()
        print(f'RPC[RequestVote] Invoked')
        print(f'\tArgs:')
        print(f'\t\tcandidate_id: {request.candidate_id}')
        print(f'\t\tcandidate_term: {request.candidate_term}')

        if request.candidate_term > node.term:
            node.term = request.candidate_term
            node.votedFor = -1

        if request.candidate_term < node.term or node.votedFor != -1:
            return pb2.RequestVoteResponse(**{"term": node.term, "vote_result": False})

        print(f'Voted for NODE {request.candidate_id}')

        node.becomeFollower(request.candidate_term, request.candidate_id)

        return pb2.RequestVoteResponse(**{"term": node.term, "vote_result": True})

    def GetLeader(self, request, context):
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.GetLeaderResponse()
        print(f'RPC[GetLeader] Invoked')
        return pb2.GetLeaderResponse(**{"leader_id": node.leaderId})
        
    def AddValue(self, request, context):
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.AddValueResponse()

        print(f'RPC[AddValue] Invoked')
        print(f'\tArgs:')
        print(f'\t\tvalue_to_add: {request.value_to_add}')

        if node.id != node.leaderId:
            stub = get_stub(SERVERS_INFO[node.leaderId])
            response = stub.AddValue(request)
            return response

        node.uncommited_value += request.value_to_add

        return pb2.AddValueResponse()
    
    def GetValue(self, request, context):
        if SUSPEND:
            context.set_details("Server suspended")
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return pb2.GetValueResponse()
        print(f'RPC[GetValue] Invoked')
        return pb2.GetValueResponse(**{"value": node.committed_value})

    def Suspend(self, request, context):
        global SUSPENDED
        print(f'RPC[Suspend] Invoked')
        node.suspended = True
        SUSPENDED = True
        return pb2.SuspendResponse(**{})
    
    def Resume(self, request, context):
        global SUSPENDED
        print(f'RPC[Resume] Invoked')
        node.suspended = False
        SUSPENDED = False
        return pb2.ResumeResponse(**{})

def loop():
    global node
    node.handleCurrentState()

    time.sleep(0.001)
    node.timer += 1

# ----------------------------- Do not change ----------------------------- 
def serve():
    global node
    print(f'NODE {NODE_ID} | {SERVERS_INFO[NODE_ID]}')
    node = Node()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_RaftNodeServicer_to_server(Handler(), server)
    server.add_insecure_port(SERVERS_INFO[NODE_ID])
    try:
        server.start()
        while True:
            loop()
    except grpc.RpcError as e:
        print(f"Unexpected Error: {e}")
    except KeyboardInterrupt:
        server.stop(grace=10)
        print("Shutting Down...")


def init(node_id):
    global NODE_ID
    NODE_ID = node_id

    with open("config.conf") as f:
        global SERVERS_INFO
        lines = f.readlines()
        for line in lines:
            parts = line.split()
            id, address = parts[0], parts[1]
            SERVERS_INFO[int(id)] = str(address)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("node_id", type=int)
    args = parser.parse_args()

    init(args.node_id)
    serve()
# ----------------------------- Do not change -----------------------------
