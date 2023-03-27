package surfstore

import (
	context "context"
	"fmt"
	"log"
	"sync"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation

	metaStore *MetaStore

	id             int64
	ip             string
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64
	lastApplied    int64

	// for leader to check follower state
	nextIndex  []int64
	matchIndex []int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// check crush status
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// there is no write happening here
	needUpdate := false
	commitChan := make(chan bool)
	//put in the loop because this is read case, just retry for server recover

	go s.sendToFollowerForMajority(ctx, needUpdate, commitChan)
	getMajority := <-commitChan
	if getMajority {
		return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
	}
	return nil, nil

}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, c *BlockHashes) (*BlockStoreMap, error) {
	// check crush status
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// there is no write happening here
	needUpdate := false
	commitChan := make(chan bool)
	//put in the loop because this is read case, just retry for server recover

	go s.sendToFollowerForMajority(ctx, needUpdate, commitChan)
	getMajority := <-commitChan
	if getMajority {
		return s.metaStore.GetBlockStoreMap(ctx, c)
	}
	return nil, nil

}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// check crush status
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// there is no write happening here
	needUpdate := false
	commitChan := make(chan bool)
	//put in the loop because this is read case, just retry for server recover

	go s.sendToFollowerForMajority(ctx, needUpdate, commitChan)
	getMajority := <-commitChan
	if getMajority {
		return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	}
	return nil, nil

}

//recovery log expectaton

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// check crush status
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)
	//same chanel count

	// send entry to all followers in parallel
	needUpdate := true

	go s.sendToFollowerForMajority(ctx, needUpdate, commitChan)

	// keep trying indefinitely (even after responding) ** rely on sendheartbeat
	updateChan := make(chan bool, len(s.peers)-1)
	for id, serverAddr := range s.peers {
		if id == int(s.id) {
			continue
		}

		currFollowerNextIndex := s.nextIndex[id]
		currLogIndex := len(s.log) - 1

		if int64(currLogIndex) >= currFollowerNextIndex {
			// update entry for followers
			go s.keepUpdatingFollowers(ctx, int64(id), serverAddr, updateChan)

		}

	}

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan
	if commit {

		// once committed, leader can apply to the state machine
		version, err := s.metaStore.UpdateFile(ctx, filemeta)
		return version, err
	}
	return nil, fmt.Errorf("time out")
}

func (s *RaftSurfstore) keepUpdatingFollowers(ctx context.Context, id int64, addr string, updateChan chan bool) {

	signalUpdate := true
	for {
		// check crush status
		s.isCrashedMutex.RLock()
		isCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()

		// leader crash
		if isCrashed {

			break
		}

		// check leader status
		s.isLeaderMutex.RLock()
		isLeader := s.isLeader
		s.isLeaderMutex.RUnlock()

		if !isLeader {

			break
		}

		finish := make(chan int)

		prevLogIndex := s.nextIndex[id] - 1
		if s.nextIndex[id] < 0 {
			prevLogIndex = -1
		}

		prevLogTerm := int64(0)

		if prevLogIndex >= 0 {
			prevLogTerm = s.log[prevLogIndex].Term
		}

		updateAppendEntriesInput := AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}

		go s.sendToFollower(ctx, int64(id), addr, signalUpdate, finish, &updateAppendEntriesInput)

		finished := <-finish
		if finished == 1 {
			updateChan <- true
			return
		} else if finished == -1 { // means that server is not just not uptodate, but crashed so no point to keep going

			updateChan <- false
			return

		}
	}

}

func (s *RaftSurfstore) sendToFollowerForMajority(ctx context.Context, needUpdate bool, commitChan chan bool) { //use for read write commit
	signalUpdate := false
	// contact all the follower, send some AppendEntries call

	// this is only needed when needUpdate is true

	majorityAppendEntriesInput := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  0,
		PrevLogIndex: 0,
		Entries:      make([]*UpdateOperation, 0),
		LeaderCommit: s.commitIndex,
	}

	if needUpdate {
		currLogIndex := int64(len(s.log)) - 1
		prevLogIndex := currLogIndex - 1
		prevLogTerm := int64(0)

		if s.commitIndex >= 0 {
			prevLogTerm = s.log[prevLogIndex].Term
		}
		majorityAppendEntriesInput.Entries = s.log
		majorityAppendEntriesInput.PrevLogTerm = prevLogTerm
		majorityAppendEntriesInput.PrevLogIndex = prevLogIndex
	}

	// wait in loop for responses

	for {
		totalResponses := 1
		totalAppends := 1

		responses := make(chan int, len(s.peers)-1)
		for id, addr := range s.peers {
			if int64(id) == s.id {
				continue
			}

			go s.sendToFollower(ctx, int64(id), addr, signalUpdate, responses, &majorityAppendEntriesInput)
		}
		result := <-responses
		s.isCrashedMutex.RLock()
		isCrashed := s.isCrashed
		s.isCrashedMutex.RUnlock()

		if isCrashed {
			break
		}
		totalResponses++
		if result == 1 {
			totalAppends++
		}

		if totalAppends > len(s.peers)/2 {

			commitChan <- true

			if needUpdate {

				s.commitIndex = int64(len(s.log) - 1)
				break
			}

		}
		if totalResponses == len(s.peers) {

			totalResponses = 0

		}

	}

}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, id int64, addr string, signalUpdate bool, responses chan int, input *AppendEntryInput) {
	// check crush status
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		fmt.Printf("Follower Server Crashed, error: %v\n", ERR_SERVER_CRASHED)
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		fmt.Printf("Not leader! error: %v\n", ERR_NOT_LEADER)

	}

	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)

	output, _ := client.AppendEntries(ctx, input)

	//majority read check case
	if len(input.Entries) == 0 {

		if output != nil && output.Success {
			responses <- 1
		} else {
			responses <- -1
		}
	} else {

		// update file case
		if output != nil {

			s.nextIndex[id] = output.MatchedIndex + 1
			s.matchIndex[id] = output.MatchedIndex

			// keep updating file case
			if signalUpdate {

				if output.Success && s.matchIndex[id] == s.commitIndex {
					responses <- 1
				} else {

					if output.Term <= s.term {
						s.nextIndex[id] = s.nextIndex[id] - 1
					} else {
						s.term = output.Term
						s.isLeaderMutex.Lock()
						s.isLeader = false
						s.isLeaderMutex.Unlock()

					}
					responses <- 0
				}

				// check majority for file update case
			} else {

				if output.Success && s.nextIndex[id] >= s.commitIndex {

					responses <- 1

				} else {

					if output.Term > s.term {
						s.term = output.Term
						s.isLeaderMutex.Lock()
						s.isLeader = false
						s.isLeaderMutex.Unlock()

					}
				}
			}
		} else {
			responses <- -1
		}

	}
	conn.Close()

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED

	}

	// read situation
	if len(input.Entries) == 0 {

		if input.Term < s.term {
			// 1. Reply false if term < currentTerm (§5.1)
			// means this "follower" was actually elected as new leader, so new leader rejects it
			output := &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: 0}
			return output, nil

		} else {
			// could only be old followers or old leader
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()

			s.term = input.Term
		}

		output := &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: !isCrashed, MatchedIndex: 0}
		return output, nil

	}

	if input.Term < s.term {
		// 1. Reply false if term < currentTerm (§5.1)
		// means this "follower" was actually elected as new leader, so new leader rejects it
		output := &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: 0}
		return output, nil

	} else {
		// could only be old followers or old leader
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.isLeaderMutex.Unlock()

		s.term = input.Term

		//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		if input.PrevLogIndex >= int64(len(s.log)) {

			output := &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: false, MatchedIndex: 0}
			return output, nil
		}

		if input.PrevLogIndex >= 0 {

			//3. If an existing entry conflicts with a new one (same index but different
			// terms), delete the existing entry and all that follow it (§5.3)
			if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
				s.log = s.log[:input.PrevLogIndex]
			}
			s.log = s.log[:input.PrevLogIndex+1]
			//todo
		}
		// 4. Append any new entries not already in the log

		s.log = input.Entries

		/// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
		// of last new entry)

		if input.LeaderCommit > s.commitIndex {

			if input.LeaderCommit > int64(len(s.log)-1) {
				s.commitIndex = int64(len(s.log) - 1)
			} else {
				s.commitIndex = input.LeaderCommit
			}

		}

		for s.lastApplied < input.LeaderCommit {
			entry := s.log[s.lastApplied+1]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
			s.lastApplied++
		}

		s.commitIndex = s.lastApplied
		output := &AppendEntryOutput{ServerId: s.id, Term: s.term, Success: true, MatchedIndex: s.commitIndex}
		return output, nil

	}

}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	//.Println("severId", s.id, "setleader")
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	// Volatile state on leaders: (Reinitialized after election)

	for id := range s.peers {

		s.nextIndex[id] = int64(s.commitIndex + 1)
		s.matchIndex[id] = 0 // initialize to 0, check on next appendEntry

	}
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// check crush status

	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()

	if isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// check leader status
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()

	if !isLeader {
		return nil, ERR_NOT_LEADER
	}
	//make sures self is uptodate with log

	for s.lastApplied < int64(s.commitIndex) {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}

	s.commitIndex = s.lastApplied
	/*
		prevLogIndex := int64(len(s.log) - 1)
		prevLogTerm := int64(0)
		if prevLogIndex >= 0 {
			prevLogTerm = s.log[prevLogIndex].Term
		}

		heartBeatAppendEntriesInput := AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: prevLogIndex,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}*/

	//signalUpdate := true

	for {
		responses := make(chan bool, len(s.peers)-1)
		for id, follower := range s.peers {
			if int64(id) == s.id {
				continue
			}

			go s.keepUpdatingFollowers(ctx, int64(id), follower, responses)

		}

		totalResponses := 1
		totalAppends := 1

		// wait in loop for responses

		for {

			result := <-responses
			totalResponses++

			if result {
				totalAppends++
			}
			if totalResponses == len(s.peers) {
				break
			}

		}
		if totalAppends > len(s.peers)/2 {
			return &Success{Flag: true}, nil

		}

	}

}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {

	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	log.Println("severId", s.id, "GetInternalState", state.IsLeader, state.Term, state.Log, state.MetaMap)
	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
