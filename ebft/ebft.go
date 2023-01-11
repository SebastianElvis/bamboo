package ebft

import (
	"fmt"

	"github.com/gitferry/bamboo/blockchain"
	"github.com/gitferry/bamboo/config"
	"github.com/gitferry/bamboo/crypto"
	"github.com/gitferry/bamboo/election"
	"github.com/gitferry/bamboo/log"
	"github.com/gitferry/bamboo/message"
	"github.com/gitferry/bamboo/node"
	"github.com/gitferry/bamboo/pacemaker"
	"github.com/gitferry/bamboo/types"
)

type Ebft struct {
	node.Node
	election.Election
	pm                     *pacemaker.Pacemaker
	bc                     *blockchain.BlockChain
	notarizedChain         [][]*blockchain.Block
	bufferedBlocks         map[crypto.Identifier]*blockchain.Block
	bufferedQCs            map[crypto.Identifier]*blockchain.QC
	bufferedNotarizedBlock map[crypto.Identifier]*blockchain.QC
	committedBlocks        chan *blockchain.Block
	forkedBlocks           chan *blockchain.Block
	echoedBlock            map[crypto.Identifier]struct{}
	echoedVote             map[crypto.Identifier]struct{}
}

// NewEbft creates a new Ebft instance
func NewEbft(
	node node.Node,
	pm *pacemaker.Pacemaker,
	elec election.Election,
	committedBlocks chan *blockchain.Block,
	forkedBlocks chan *blockchain.Block) *Ebft {
	eb := new(Ebft)
	eb.Node = node
	eb.Election = elec
	eb.pm = pm
	eb.committedBlocks = committedBlocks
	eb.forkedBlocks = forkedBlocks
	eb.bc = blockchain.NewBlockchain(config.GetConfig().N())
	eb.bufferedBlocks = make(map[crypto.Identifier]*blockchain.Block)
	eb.bufferedQCs = make(map[crypto.Identifier]*blockchain.QC)
	eb.bufferedNotarizedBlock = make(map[crypto.Identifier]*blockchain.QC)
	eb.notarizedChain = make([][]*blockchain.Block, 0)
	eb.echoedBlock = make(map[crypto.Identifier]struct{})
	eb.echoedVote = make(map[crypto.Identifier]struct{})
	eb.pm.AdvanceView(0)
	return eb
}

// ProcessBlock processes an incoming block as follows:
// 1. check if the view of the block matches current view (ignore for now)
// 2. check if the view of the block matches the proposer's view (ignore for now)
// 3. insert the block into the block tree
// 4. if the view of the block is lower than the current view, don't vote
// 5. if the block is extending the longest notarized chain, vote for the block
// 6. if the view of the block is higher than the the current view, buffer the block
// and process it when entering that view
func (eb *Ebft) ProcessBlock(block *blockchain.Block) error {
	if eb.bc.Exists(block.ID) {
		return nil
	}
	log.Debugf("[%v] is processing block, view: %v, id: %x", eb.ID(), block.View, block.ID)
	curView := eb.pm.GetCurView()
	if block.View < curView {
		return fmt.Errorf("received a stale block")
	}
	_, err := eb.bc.GetBlockByID(block.PrevID)
	if err != nil && block.View > 1 {
		// buffer future blocks
		eb.bufferedBlocks[block.PrevID] = block
		log.Debugf("[%v] buffer the block for future processing, view: %v, id: %x", eb.ID(), block.View, block.ID)
		return nil
	}
	if !eb.Election.IsLeader(block.Proposer, block.View) {
		return fmt.Errorf("received a proposal (%v) from an invalid leader (%v)", block.View, block.Proposer)
	}
	if block.Proposer != eb.ID() {
		blockIsVerified, _ := crypto.PubVerify(block.Sig, crypto.IDToByte(block.ID), block.Proposer)
		if !blockIsVerified {
			log.Warningf("[%v] received a block with an invalid signature", eb.ID())
		}
	}
	_, exists := eb.echoedBlock[block.ID]
	if !exists {
		eb.echoedBlock[block.ID] = struct{}{}
		eb.Broadcast(block)
	}
	eb.bc.AddBlock(block)
	shouldVote := eb.votingRule(block)
	if !shouldVote {
		log.Debugf("[%v] is not going to vote for block, id: %x", eb.ID(), block.ID)
		eb.bufferedBlocks[block.PrevID] = block
		log.Debugf("[%v] buffer the block for future processing, view: %v, id: %x", eb.ID(), block.View, block.ID)
		return nil
	}
	vote := blockchain.MakeVote(block.View, eb.ID(), block.ID)
	// vote to the current leader
	eb.ProcessVote(vote)
	eb.Broadcast(vote)

	// process buffers
	qc, ok := eb.bufferedQCs[block.ID]
	if ok {
		eb.processCertificate(qc)
	}
	b, ok := eb.bufferedBlocks[block.ID]
	if ok {
		_ = eb.ProcessBlock(b)
	}
	return nil
}

func (eb *Ebft) ProcessVote(vote *blockchain.Vote) {
	log.Debugf("[%v] is processing the vote, block id: %x", eb.ID(), vote.BlockID)
	if vote.Voter != eb.ID() {
		voteIsVerified, err := crypto.PubVerify(vote.Signature, crypto.IDToByte(vote.BlockID), vote.Voter)
		if err != nil {
			log.Fatalf("[%v] Error in verifying the signature in vote id: %x", eb.ID(), vote.BlockID)
			return
		}
		if !voteIsVerified {
			log.Warningf("[%v] received a vote with invalid signature. vote id: %x", eb.ID(), vote.BlockID)
			return
		}
	}
	// echo the message
	_, exists := eb.echoedBlock[vote.BlockID]
	if !exists {
		eb.echoedBlock[vote.BlockID] = struct{}{}
		eb.Broadcast(vote)
	}
	isBuilt, qc := eb.bc.AddVote(vote)
	if !isBuilt {
		log.Debugf("[%v] votes are not sufficient to build a qc, view: %v, block id: %x", eb.ID(), vote.View, vote.BlockID)
		return
	}
	// send the QC to the next leader
	log.Debugf("[%v] a qc is built, view: %v, block id: %x", eb.ID(), qc.View, qc.BlockID)
	eb.processCertificate(qc)
}

func (eb *Ebft) ProcessRemoteTmo(tmo *pacemaker.TMO) {
	log.Debugf("[%v] is processing tmo from %v", eb.ID(), tmo.NodeID)
	isBuilt, tc := eb.pm.ProcessRemoteTmo(tmo)
	if !isBuilt {
		log.Debugf("[%v] not enough tc for %v", eb.ID(), tmo.View)
		return
	}
	log.Debugf("[%v] a tc is built for view %v", eb.ID(), tc.View)
	eb.processTC(tc)
}

func (eb *Ebft) ProcessLocalTmo(view types.View) {
	tmo := &pacemaker.TMO{
		View:   view,
		NodeID: eb.ID(),
	}
	eb.Broadcast(tmo)
	eb.ProcessRemoteTmo(tmo)
}

func (eb *Ebft) MakeProposal(view types.View, payload []*message.Transaction) *blockchain.Block {
	prevID := eb.forkChoice()
	block := blockchain.MakeBlock(view, &blockchain.QC{
		View:      0,
		BlockID:   prevID,
		AggSig:    nil,
		Signature: nil,
	}, prevID, payload, eb.ID())
	return block
}

func (eb *Ebft) forkChoice() crypto.Identifier {
	var prevID crypto.Identifier
	if eb.GetNotarizedHeight() == 0 {
		prevID = crypto.MakeID("Genesis block")
	} else {
		tailNotarizedBlock := eb.notarizedChain[eb.GetNotarizedHeight()-1][0]
		prevID = tailNotarizedBlock.ID
	}
	return prevID
}

func (eb *Ebft) processTC(tc *pacemaker.TC) {
	if tc.View < eb.pm.GetCurView() {
		return
	}
	go eb.pm.AdvanceView(tc.View)
}

// 1. advance view
// 2. update notarized chain
// 3. check commit rule
// 4. commit blocks
func (eb *Ebft) processCertificate(qc *blockchain.QC) {
	log.Debugf("[%v] is processing a qc, view: %v, block id: %x", eb.ID(), qc.View, qc.BlockID)
	if qc.View < eb.pm.GetCurView() {
		return
	}
	_, err := eb.bc.GetBlockByID(qc.BlockID)
	if err != nil && qc.View > 1 {
		log.Debugf("[%v] buffered the QC, view: %v, id: %x", eb.ID(), qc.View, qc.BlockID)
		eb.bufferedQCs[qc.BlockID] = qc
		return
	}
	if qc.Leader != eb.ID() {
		quorumIsVerified, _ := crypto.VerifyQuorumSignature(qc.AggSig, qc.BlockID, qc.Signers)
		if quorumIsVerified == false {
			log.Warningf("[%v] received a quorum with invalid signatures", eb.ID())
			return
		}
	}
	err = eb.updateNotarizedChain(qc)
	if err != nil {
		// the corresponding block does not exist
		log.Debugf("[%v] cannot notarize the block, %x: %w", eb.ID(), qc.BlockID, err)
		return
	}
	eb.pm.AdvanceView(qc.View)
	if qc.View < 3 {
		return
	}
	ok, block := eb.commitRule()
	if !ok {
		return
	}
	committedBlocks, forkedBlocks, err := eb.bc.CommitBlock(block.ID, eb.pm.GetCurView())
	if err != nil {
		log.Errorf("[%v] cannot commit blocks", eb.ID())
		return
	}
	for _, cBlock := range committedBlocks {
		eb.committedBlocks <- cBlock
		delete(eb.echoedBlock, cBlock.ID)
		delete(eb.echoedVote, cBlock.ID)
		log.Debugf("[%v] is going to commit block, view: %v, id: %x", eb.ID(), cBlock.View, cBlock.ID)
	}
	for _, fBlock := range forkedBlocks {
		eb.forkedBlocks <- fBlock
		log.Debugf("[%v] is going to collect forked block, view: %v, id: %x", eb.ID(), fBlock.View, fBlock.ID)
	}
	b, ok := eb.bufferedBlocks[qc.BlockID]
	if ok {
		log.Debugf("[%v] found a buffered block by qc, qc.BlockID: %x", eb.ID(), qc.BlockID)
		_ = eb.ProcessBlock(b)
		delete(eb.bufferedBlocks, qc.BlockID)
	}
	qc, ok = eb.bufferedNotarizedBlock[qc.BlockID]
	if ok {
		log.Debugf("[%v] found a bufferred qc, view: %v, block id: %x", eb.ID(), qc.View, qc.BlockID)
		eb.processCertificate(qc)
		delete(eb.bufferedQCs, qc.BlockID)
	}
}

func (eb *Ebft) updateNotarizedChain(qc *blockchain.QC) error {
	block, err := eb.bc.GetBlockByID(qc.BlockID)
	if err != nil {
		return fmt.Errorf("cannot find the block")
	}
	// check the last block in the notarized chain
	// could be improved by checking view
	if eb.GetNotarizedHeight() == 0 {
		log.Debugf("[%v] is processing the first notarized block, view: %v, id: %x", eb.ID(), qc.View, qc.BlockID)
		newArray := make([]*blockchain.Block, 0)
		newArray = append(newArray, block)
		eb.notarizedChain = append(eb.notarizedChain, newArray)
		return nil
	}
	for i := eb.GetNotarizedHeight() - 1; i >= 0 || i >= eb.GetNotarizedHeight()-3; i-- {
		lastBlocks := eb.notarizedChain[i]
		for _, b := range lastBlocks {
			if b.ID == block.PrevID {
				var blocks []*blockchain.Block
				if i < eb.GetNotarizedHeight()-1 {
					blocks = make([]*blockchain.Block, 0)
				}
				blocks = append(blocks, block)
				eb.notarizedChain = append(eb.notarizedChain, blocks)
				return nil
			}
		}
	}
	eb.bufferedNotarizedBlock[block.PrevID] = qc
	log.Debugf("[%v] the parent block is not notarized, buffered for now, view: %v, block id: %x", eb.ID(), qc.View, qc.BlockID)
	return fmt.Errorf("the block is not extending the notarized chain")
}

func (eb *Ebft) GetChainStatus() string {
	chainGrowthRate := eb.bc.GetChainGrowth()
	blockIntervals := eb.bc.GetBlockIntervals()
	return fmt.Sprintf("[%v] The current view is: %v, chain growth rate is: %v, ave block interval is: %v", eb.ID(), eb.pm.GetCurView(), chainGrowthRate, blockIntervals)
}

func (eb *Ebft) GetNotarizedHeight() int {
	return len(eb.notarizedChain)
}

// 1. get the tail of the longest notarized chain (could be more than one)
// 2. check if the block is extending one of them
func (eb *Ebft) votingRule(block *blockchain.Block) bool {
	if block.View <= 2 {
		return true
	}
	lastBlocks := eb.notarizedChain[eb.GetNotarizedHeight()-1]
	for _, b := range lastBlocks {
		if block.PrevID == b.ID {
			return true
		}
	}

	return false
}

// 1. get the last three blocks in the notarized chain
// 2. check if they are consecutive
// 3. if so, return the second block to commit
func (eb *Ebft) commitRule() (bool, *blockchain.Block) {
	height := eb.GetNotarizedHeight()
	if height < 3 {
		return false, nil
	}
	lastBlocks := eb.notarizedChain[height-1]
	if len(lastBlocks) != 1 {
		return false, nil
	}
	lastBlock := lastBlocks[0]
	secondBlocks := eb.notarizedChain[height-2]
	if len(secondBlocks) != 1 {
		return false, nil
	}
	secondBlock := secondBlocks[0]
	firstBlocks := eb.notarizedChain[height-3]
	if len(firstBlocks) != 1 {
		return false, nil
	}
	firstBlock := firstBlocks[0]
	// check three-chain
	if ((firstBlock.View + 1) == secondBlock.View) && ((secondBlock.View + 1) == lastBlock.View) {
		return true, secondBlock
	}
	return false, nil
}
