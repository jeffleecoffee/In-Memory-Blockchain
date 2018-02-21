package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"sort"
)

var NODEID int

var NUMZEROES int

var GENESISBLOCKHASH Hash

var numNodes = 0
var nodeIPs map[int]Node
var clientIP string
var clientIPMutex sync.Mutex

var debug *log.Logger
var computeTxChan chan Block
var doneTxCompute chan bool
var addBlockDone chan bool

type Node struct {
	NodeIpAndPort string
	Client        *rpc.Client
}

var NODELISTENIP string
var CLIENTLISTENIP string

var BlockChain map[Hash]Block

var TransactionQueue []Block

type Block struct {
	PrevHash           Hash
	Client             string
	LocalTransactionID int
	Transaction        []Log
	NodeID             int
	ValidateNum        int
	Validated		   bool
	Children           []Hash
	ChainLen           int
	Nonce              uint32
}

type Key string
type Value string
type Hash string

var KeyValueTable map[Key]Value

var ClientStateTable map[string]ClientState

type ClientState struct {
	Alive              bool
	Address            string
	Client             *rpc.Client
	Logs               []Log
	Holds              []Key
	LocalTransactionID int
}

type Log struct {
	Action string
	Key    Key
	Value  Value
}

type NewTransactionReq struct {
	Client             string
	LocalTransactionID int
}

type PutReq struct {
	Client string
	Key    Key
	Value  Value
}

type GetReq struct {
	Client string
	Key    Key
}

type GetRes struct {
	Value Value
}

type StartWorkReq struct {
	Client      string
	ValidateNum int
}

type ValidateRes struct {
	TransactionID int
	LocalTransactionID int
}

// Node server type (for RPC)
type NodeToNode int

func (n *NodeToNode) AddNewBlock(block Block, reply *bool) error {
	if alreadyInBlockChain(block) {
		*reply = true
		return nil
	}

	fmt.Println("AddNewBlock from node: ", block.NodeID)

	blockByte, err := blockToByte(block)
	if err != nil {
		debug.Println(err)
	}
	sum := sha256.Sum256(blockByte)
	hashStr := fmt.Sprintf("%x", sum)

	if leadingZeros(hashStr) == NUMZEROES { // validate that the leading zero count is correct
		addNewBlock(block, hashStr)
		// printKeyValueTable()

		var success bool
		for nodeID, node := range nodeIPs { //Ask all other nodes except block creator and this node to add block
			if nodeID != NODEID && nodeID != block.NodeID{
				err = node.Client.Call("NodeToNode.AddNewBlock", block, &success)
				if err != nil {
					continue //TODO maybe take out that node if node to node communication failed?
				} 
			}
		}

		*reply = true
	} else {
		*reply = false
	}
	if len(block.Client) != 0 {
		addBlockDone <- true
	}
	return nil
}

// Node server type (for RPC)
type ClientToNode int

func (c *ClientToNode) ClientListenAddress(clientPort string, reply *bool) error {
	// clientIPAndPort := net.JoinHostPort(clientIP, clientPort)
	clientIPMutex.Unlock()

	fmt.Println("ClientListenAddress Request Received from ClientID: ", clientPort)

	client, err := rpc.Dial("tcp", clientPort)
	checkError("", err, true)

	ClientStateTable[clientPort] = ClientState{
		Alive:   true,
		Address: clientPort,
		Client:  client,
	}
	fmt.Println("ClientStateTable: ", ClientStateTable)
	*reply = true
	return nil
}

func (c *ClientToNode) ClientNewTransaction(newTransactionReq NewTransactionReq, reply *bool) error {

	fmt.Println("ClientListenAddress Request Received from ClientID: ", newTransactionReq.Client)

	ClientStateTable[newTransactionReq.Client] = ClientState{
		Alive:              ClientStateTable[newTransactionReq.Client].Alive,
		Address:            ClientStateTable[newTransactionReq.Client].Address,
		Client:             ClientStateTable[newTransactionReq.Client].Client,
		Logs:               make([]Log, 0),
		Holds:              make([]Key, 0),
		LocalTransactionID: newTransactionReq.LocalTransactionID,
	}
	fmt.Println("ClientStateTable: ", ClientStateTable)
	*reply = true
	return nil
}

func (c *ClientToNode) Get(req GetReq, reply *GetRes) error {
	fmt.Println("Get Request Received from ClientID: ", req.Client)
	fmt.Println("ClientStateTable: ", ClientStateTable)

	log := Log{
		Action: "Get",
		Key:    req.Key,
	}

	// fmt.Println("isKeyAlreadyHeld(req.Key, req.Client): ", isKeyAlreadyHeld(req.Key, req.Client))

	if isKeyAlreadyHeld(req.Key, req.Client) {
		ClientStateTable[req.Client] = ClientState{
			Alive:              ClientStateTable[req.Client].Alive,
			Address:            ClientStateTable[req.Client].Address,
			Client:             ClientStateTable[req.Client].Client,
			Logs:               append(ClientStateTable[req.Client].Logs, log),
			Holds:              ClientStateTable[req.Client].Holds,
			LocalTransactionID: ClientStateTable[req.Client].LocalTransactionID,
		}
		*reply = GetRes{
			Value: KeyValueTable[req.Key],
		}
		for _, log := range ClientStateTable[req.Client].Logs {
			if log.Action == "Put" && req.Key == log.Key {
				*reply = GetRes{
					Value: log.Value,
				}
			}
		}
		fmt.Println(reply.Value)
		fmt.Println(ClientStateTable[req.Client].Logs)
	} else {
		ClientStateTable[req.Client] = ClientState{
			Alive:              ClientStateTable[req.Client].Alive,
			Address:            ClientStateTable[req.Client].Address,
			Client:             ClientStateTable[req.Client].Client,
			Logs:               append(ClientStateTable[req.Client].Logs, log),
			Holds:              append(ClientStateTable[req.Client].Holds, req.Key),
			LocalTransactionID: ClientStateTable[req.Client].LocalTransactionID,
		}
		*reply = GetRes{
			Value: KeyValueTable[req.Key],
		}
	}
	return nil
}

func (c *ClientToNode) Put(req PutReq, reply *bool) error {
	fmt.Println("Put Request Received from ClientID: ", req.Client)
	fmt.Println("Put: ", req.Key)

	log := Log{
		Action: "Put",
		Key:    req.Key,
		Value:  req.Value,
	}

	// fmt.Println("isKeyAlreadyHeld(req.Key, req.Client): ", isKeyAlreadyHeld(req.Key, req.Client))

	if isKeyAlreadyHeld(req.Key, req.Client) {
		ClientStateTable[req.Client] = ClientState{
			Alive:              ClientStateTable[req.Client].Alive,
			Address:            ClientStateTable[req.Client].Address,
			Client:             ClientStateTable[req.Client].Client,
			Logs:               append(ClientStateTable[req.Client].Logs, log),
			Holds:              ClientStateTable[req.Client].Holds,
			LocalTransactionID: ClientStateTable[req.Client].LocalTransactionID,
		}
	} else {
		ClientStateTable[req.Client] = ClientState{
			Alive:              ClientStateTable[req.Client].Alive,
			Address:            ClientStateTable[req.Client].Address,
			Client:             ClientStateTable[req.Client].Client,
			Logs:               append(ClientStateTable[req.Client].Logs, log),
			Holds:              append(ClientStateTable[req.Client].Holds, req.Key),
			LocalTransactionID: ClientStateTable[req.Client].LocalTransactionID,
		}
	}
	*reply = true
	return nil
}

func (c *ClientToNode) StartWork(req StartWorkReq, reply *bool) error {
	fmt.Println("StartWork Request Received from ClientID: ", req.Client)

	clientTransactions := make([]Log, 0)
	for _, log := range ClientStateTable[req.Client].Logs {
		if log.Action == "Put" {
			clientTransactions = append(clientTransactions, log)
		}
	}

	block := Block{
		Client:             req.Client,
		LocalTransactionID: ClientStateTable[req.Client].LocalTransactionID,
		Transaction:        clientTransactions,
		NodeID:             NODEID,
		ValidateNum:        req.ValidateNum,
		Validated: 			false,
		Children:           make([]Hash, 0),
		Nonce:              0,
	}

	TransactionQueue = append(TransactionQueue, block)

	*reply = true
	return nil
}

func (c *ClientToNode) Abort(clientIpAndPort string, reply *bool) error {
	fmt.Println("Abort Request Received from ClientID: ", clientIpAndPort)

	abortTransaction(clientIpAndPort)

	// printKeyValueTable() //REMOVE

	*reply = true
	return nil
}

func (c *ClientToNode) GetChildren(reqHash string, reply *[]string) error {
	fmt.Println("GetChildren Request Received for hash: ", reqHash)

	if reqHash == "" {
		reqHash = string(GENESISBLOCKHASH)
	}
	for _, childHash := range BlockChain[Hash(reqHash)].Children {
		*reply = append(*reply, string(childHash))
	}
	
	return nil
}

func main() {
	args := os.Args[1:]

	// Missing command line args.
	if len(args) != 6 {
		fmt.Println("Usage: go run kvnode.go [ghash] [num-zeroes] [nodesFile] [nodeID] [listen-node-in IP:port] [listen-client-in IP:port]")
		return
	}

	nodeIPs = make(map[int]Node)

	pF, err := os.Open(args[2])
	checkError("nodesFile argument cannot be opened", err, true)
	bufIo := bufio.NewReader(pF)
	nodeIndex := 1
	for {
		nodeIP, err := bufIo.ReadString('\n')
		nodeIP = strings.TrimSpace(nodeIP)
		if err == io.EOF {
			break
		}
		nodeIPs[nodeIndex] = Node{
			NodeIpAndPort: nodeIP,
		}
		nodeIndex++
		numNodes++
	}

	fmt.Println("NodeIPs: ", nodeIPs)

	nodeID, err := strconv.Atoi(args[3])
	checkError("nodeID argument cannot be made into integer", err, true)
	NODEID = nodeID

	numZeroes, err := strconv.Atoi(args[1])
	checkError("num-zeroes argument cannot be made into integer", err, true)
	NUMZEROES = numZeroes

	NODELISTENIP = args[4]
	CLIENTLISTENIP = args[5]

	GENESISBLOCKHASH = Hash(args[0])

	genesisBlock := Block{
		ValidateNum: 0,
		Validated:   true,
		Children:    make([]Hash, 0),
		ChainLen:    1,
	}

	BlockChain = make(map[Hash]Block)
	BlockChain[GENESISBLOCKHASH] = genesisBlock

	done := make(chan int)
	ClientStateTable = make(map[string]ClientState)
	KeyValueTable = make(map[Key]Value)
	TransactionQueue = make([]Block, 0)

	debug = log.New(os.Stderr, "[Node "+strconv.Itoa(NODEID)+"] ", log.Lshortfile)
	computeTxChan = make(chan Block, 1)
	doneTxCompute = make(chan bool, 1)
	addBlockDone = make(chan bool, 1)

	// Set up RPC so nodes can talk to each other
	go func() {
		nServer := rpc.NewServer()
		n := new(NodeToNode)
		nServer.Register(n)

		l, err := net.Listen("tcp", NODELISTENIP)
		checkError("", err, true)
		for {
			conn, err := l.Accept()
			checkError("", err, false)
			go nServer.ServeConn(conn)
		}
	}()

	// Set up RPC so Clients can talk to Node
	go func() {
		nServer := rpc.NewServer()
		n := new(ClientToNode)
		nServer.Register(n)

		l, err := net.Listen("tcp", CLIENTLISTENIP)
		checkError("", err, true)
		for {
			conn, err := l.Accept()
			clientIPMutex.Lock()
			clientIP, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
			checkError("", err, false)
			go nServer.ServeConn(conn)
		}
	}()

	for i := 1; i <= numNodes; i++ {
		if i == NODEID {
			continue
		}
		// keep trying the node until its up
		for {
			pIP := nodeIPs[i].NodeIpAndPort
			client, err := rpc.Dial("tcp", pIP)
			if err == nil {
				nodeIPs[i] = Node{
					NodeIpAndPort: nodeIPs[i].NodeIpAndPort,
					Client:        client,
				}
				break
			} else {
				continue
			}
		}
		fmt.Println("client: ", nodeIPs[i])
	}

	fmt.Println("NodeIPs: ", nodeIPs)
	fmt.Println("###LISTENING AND CONNECTED TO ALL NODES###")

	go func() { //need to check the alive state of clients with ongoing transactions
		for {
			// fmt.Println("ClientStateTable: ", ClientStateTable)
			for _, client := range ClientStateTable {
				if client.Alive == true && (len(client.Logs) > 0) {
					// fmt.Println("ping client: ", client.Address)
					var success bool
					err := client.Client.Call("NodeToClient.CheckAlive", "ping", &success)
					if err != nil {
						fmt.Println("Client is unresponsive, aborting this client: ", client.Address)
						abortTransaction(client.Address)
						ClientStateTable[client.Address] = ClientState{
							Alive:              false,
							Address:            ClientStateTable[client.Address].Address,
							Client:             ClientStateTable[client.Address].Client,
							Logs:               make([]Log, 0),
							Holds:              make([]Key, 0),
							LocalTransactionID:  ClientStateTable[client.Address].LocalTransactionID,
						}
						// printKeyValueTable() //REMOVE
					}
				}
			}
			time.Sleep(2000 * time.Millisecond)
		}
	}()

	go func() { //keep dequeueing the TransactionQueue and do work
		// continue to check if there's any object in the queue, if size > 0, pop the first element and invoke the channel for computing tx block
		for {
			if len(TransactionQueue) == 0 {
				time.Sleep(1 * time.Millisecond)
				continue
			} 
			newTx := TransactionQueue[0]
			TransactionQueue = TransactionQueue[1:]
			// write tx to channel
			computeTxChan <- newTx
			// waiting for doneTxCompute to continue
			<-doneTxCompute
		}
	}()

	var noopNonce uint32
	noopNonce = 0 // initial first nonce

	for {
		select {
		case block := <-computeTxChan:
			debug.Println("Computing tx block now")
			TXLoop:
			for {
				// during process of generating transaction block, other nodes can done the work first and send message to me tell me to add block.
				// if I validated that block, I must added to the chain
				
				if len(addBlockDone) > 0 {
					debug.Println("Something in addBlockDone, must be someone told me to add block")
					// someone told me to add block and I did, stop working on this tx now
					<-addBlockDone
					doneTxCompute <- true

					// printBlockChain(BlockChain)
					break TXLoop
				}

				prevHash := getPrevHash()
				block.PrevHash = prevHash
				block.ChainLen = BlockChain[prevHash].ChainLen + 1

				blockByte, err := blockToByte(block)
				if err != nil {
					debug.Println(err)
				}
				sum := sha256.Sum256(blockByte)
				hashStr := fmt.Sprintf("%x", sum)

				// fmt.Printf("[TX] This nonce is: %v, block's hash is: %v \n", block.Nonce, hashStr)

				if leadingZeros(hashStr) == NUMZEROES {
					fmt.Println("leading zero match found for TxBlock")
					var first bool
					err := ClientStateTable[block.Client].Client.Call("NodeToClient.ProofOfWork", "", &first)
					if err != nil {
						fmt.Println("Client is unresponsive, aborting this client's transaction: ", ClientStateTable[block.Client].Address)
						abortTransaction(block.Client)
						doneTxCompute <- true
					} else if err == nil && first {
						fmt.Println("I am first")
						success := true
						for nodeID, node := range nodeIPs {
							if nodeID != NODEID {
								err = node.Client.Call("NodeToNode.AddNewBlock", block, &success)
								if err != nil {
									continue //TODO maybe take out that node if node to node communication failed?
								} else if err == nil && success == false { //if verify failed then we cannot add the block
									abortTransaction(block.Client)
									break
								}
							}
						}
						if success {
							fmt.Println("Done waiting for all other nodes to add the txn block")
							addNewBlock(block, hashStr)
						}
						doneTxCompute <- true
						fmt.Println("Done proof of work and adding block")
					} else if err == nil && !first {
						fmt.Println("I am not first so wait")
						<-addBlockDone //waiting for the block to be added
						doneTxCompute <- true
						fmt.Println("Done waiting")
					}

					break TXLoop
				}
				block.Nonce = getNextNonce(block.Nonce)

				// if NODEID == 2 { //TESTING
				// 	debug.Println("I'm node 2 so for testing I'm gonna take longer time to compute tx block")
				// 	time.Sleep(time.Millisecond * 1000 * 2)
				// }

				// time.Sleep(time.Millisecond * 1)
			}
		default:
			// generate no-op block
			noopNonce = getNextNonce(noopNonce)

			prevHash := getPrevHash()
			noopBlock := Block{
				PrevHash:			prevHash,
				NodeID:             NODEID,
				ValidateNum:        0,
				Validated:			true,
				Children:           make([]Hash, 0),
				ChainLen:			BlockChain[prevHash].ChainLen + 1,
				Nonce:              noopNonce,
			}

			blockByte, err := blockToByte(noopBlock)
			if err != nil {
				debug.Println(err)
			}
			sum := sha256.Sum256(blockByte)
			hashStr := fmt.Sprintf("%x", sum)

			// fmt.Printf("[no-op] This nonce is: %v, block's hash is: %v \n", noopNonce, hashStr)

			if leadingZeros(hashStr) == NUMZEROES {
				fmt.Println("leading zero match for noop block creation")
				success := true
				for nodeID, node := range nodeIPs {
					if nodeID != NODEID {
						err = node.Client.Call("NodeToNode.AddNewBlock", noopBlock, &success)
						if err != nil {
							continue //TODO maybe take out that node if node to node communication failed?
						} else if err == nil && success == false {
							break
						}
					}
				}
				if success {
					fmt.Println("Done waiting for all other nodes to add the no-op block")
					addNewBlock(noopBlock, hashStr)
				}
				noopNonce = 0
			}
			// time.Sleep(time.Millisecond * 1)  // TESTING set to 1 sec, should set to 10 or 100 millisec
		}
	}
	<-done
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}

func isKeyAlreadyHeld(key Key, clientAddress string) bool {
	for _, heldKey := range ClientStateTable[clientAddress].Holds {
		if heldKey == key {
			return true
		}
	}
	return false
}

func getPrevHash() (prevHash Hash) {
	prevHash = GENESISBLOCKHASH
	for blockHash, block := range BlockChain {
		if BlockChain[prevHash].ChainLen <= block.ChainLen {
			prevHash = blockHash
		}
	}
	return prevHash
}

func printKeyValueTable() {
	for key, value := range KeyValueTable {
		fmt.Println(">Key: ", key)
		fmt.Println(">>value: ", value)
	}
}

func printBlockChain(chain map[Hash]Block) {
	position := make(map[int][]Hash)
	var order []int
	// print in order
	for k, b := range chain {
		position[b.ChainLen] = append(position[b.ChainLen], k)
	}
	for i, _ := range position {
		order = append(order, i)
	}

	sort.Ints(order)

	for _, i := range order {
		loHash := position[i]
		for _, k := range loHash {
			fmt.Printf("Hash: %v \n", k)
			b := chain[k]
			fmt.Printf(">>PrevHash: %v \n", b.PrevHash)
			if len(b.Transaction) > 0 {
				// fmt.Printf("Transaction: %v \n", b.Transaction)
				for _, l := range b.Transaction {
					fmt.Printf(">>Transaction: PUT %v into %v \n", l.Value, l.Key)
				}
			}
			if len(b.Children) > 0 {
				// fmt.Printf("Transaction: %v \n", b.Transaction)
				for _, child := range b.Children {
					fmt.Printf(">>Child Hash %v\n", child)
				}
			}
			fmt.Printf(">>nodeId: %v \n", b.NodeID)
			fmt.Printf(">>Nonce: %v \n", b.Nonce)
			fmt.Printf(">>Position: %v \n", b.ChainLen)
			fmt.Printf("\n")
		}
	}

	fmt.Printf("=============================================\n")
}

func blockToByte(b Block) ([]byte, error) {
	var block_buf bytes.Buffer
	enc := gob.NewEncoder(&block_buf)
	err := enc.Encode(b.PrevHash)
	if err != nil {
		debug.Println(err.Error())
	}

	err = enc.Encode(b.Transaction)
	if err != nil {
		debug.Println(err.Error())
	}

	err = enc.Encode(b.NodeID)
	if err != nil {
		debug.Println(err.Error())
	}

	err = enc.Encode(b.Nonce)
	if err != nil {
		debug.Println(err.Error())
	}

	return block_buf.Bytes(), err
}

func leadingZeros(input string) int {
	count := 0
	for _, v := range input {
		if v == '0' {
			count++
		} else {
			break
		}
	}
	return count
}

func getNextNonce(n uint32) uint32 {
	if n == math.MaxUint32 {
		n = 0
	} else {
		n++
	}
	return n
}

func alreadyInBlockChain(block Block) bool {
	blockByte, err := blockToByte(block)
	if err != nil {
		debug.Println(err)
	}
	sum := sha256.Sum256(blockByte)
	hashStr := fmt.Sprintf("%x", sum)

	if _, ok := BlockChain[Hash(hashStr)]; !ok {
		return false
	} else {
		return true
	}
}

func addNewBlock(block Block, hashStr string) {
	// debug.Println("Right before the block added")
	// printBlockChain(BlockChain)
	// debug.Println("Right before the block added")
	// debug.Printf("\n")
	
	// add the block to BlockChain
	BlockChain[Hash(hashStr)] = block

	// debug.Println("Right after the block added")
	// printBlockChain(BlockChain)
	// debug.Println("Right after the block added")
	// debug.Printf("\n")

	for client, state := range ClientStateTable { //abort all transaction in conflict with the block
		if client != block.Client {
		clientLoop:
			for _, heldKey := range state.Holds {
				for _, log := range block.Transaction {
					if heldKey == log.Key {
						abortTransaction(client)
						fmt.Println("aborting client transaction: ", client)
						break clientLoop
					}
				}
			}
		}
	}

	for _, log := range ClientStateTable[block.Client].Logs {
		if log.Action == "Put" {
			KeyValueTable[log.Key] = log.Value
		}
	}

	ClientStateTable[block.Client] = ClientState{
		Alive: true,
		Address: ClientStateTable[block.Client].Address,
		Client: ClientStateTable[block.Client].Client,
		Logs: make([]Log, 0),
		Holds: make([]Key, 0),
	}

	for {
		if _, ok := BlockChain[block.PrevHash]; !ok {
			// previous block not exist yet due to race condition
			// debug.Println("The previous block not exist yet")
			time.Sleep(1 * time.Millisecond)
			continue
		} else {
			break
		}
	}
	
	prevBlock := BlockChain[block.PrevHash]
	BlockChain[block.PrevHash] = Block{
		PrevHash:			prevBlock.PrevHash,
		Client:             prevBlock.Client,
		LocalTransactionID: prevBlock.LocalTransactionID,
		Transaction:        prevBlock.Transaction,
		NodeID:             prevBlock.NodeID,
		ValidateNum:        prevBlock.ValidateNum,
		Validated:			prevBlock.Validated,
		Children:           append(prevBlock.Children, Hash(hashStr)),
		ChainLen:			prevBlock.ChainLen,
		Nonce:              prevBlock.Nonce,
	}

	// debug.Println("Debugging before calling validate... ")
	// printBlockChain(BlockChain)
	// debug.Println("Debugging before calling validate... ")
	// debug.Printf("\n")

	//validate previous blocks on the BlockChain
	validateBlocksOnBlockChain(hashStr, 0)

	printBlockChain(BlockChain)
	printKeyValueTable() //TESTING
}

func abortTransaction(clientIPAndPort string) {
	fmt.Println("call abort transaction")
	ClientStateTable[clientIPAndPort] = ClientState{
		Alive:              true,
		Address:            ClientStateTable[clientIPAndPort].Address,
		Client:             ClientStateTable[clientIPAndPort].Client,
		Logs:               make([]Log, 0),
		Holds:              make([]Key, 0),
		LocalTransactionID:  ClientStateTable[clientIPAndPort].LocalTransactionID,
	}

	// if it's already in the queue, that's fine... if not...
	inQueue := false
	for index:= 0; index < len(TransactionQueue); index ++ {
		block := TransactionQueue[index]
		if block.Client == clientIPAndPort {
			inQueue = true
			var reply bool
			fmt.Printf("\ntelling the client to abort...\n")
			_ = ClientStateTable[block.Client].Client.Call("NodeToClient.Abort", strconv.Itoa(block.LocalTransactionID), &reply)
			TransactionQueue = append(TransactionQueue[:index], TransactionQueue[index+1:]...)
			index--
		}
	}

	if !inQueue {
		var reply bool
		fmt.Printf("\ntelling the client to abort... NOT IN QUEUE\n")
		err := ClientStateTable[clientIPAndPort].Client.Call("NodeToClient.Abort", strconv.Itoa(ClientStateTable[clientIPAndPort].LocalTransactionID), &reply)
		if err != nil { //TODO: error can be ignored
			debug.Println("NodeToClient.Abort failed: ", err)
		}
	}

	// for index, block := range TransactionQueue {
	// 	if block.Client == clientIPAndPort {
	// 		var reply bool
	// 		_ = ClientStateTable[block.Client].Client.Call("NodeToClient.Abort", strconv.Itoa(block.LocalTransactionID), &reply)
	// 		TransactionQueue = append(TransactionQueue[:index], TransactionQueue[index+1:]...)
	// 	}
	// }
}

func validateBlocksOnBlockChain(tailHash string, numBlocksAfterTail int) {
	block := BlockChain[Hash(tailHash)]
	if (block.ValidateNum - numBlocksAfterTail <= 0) && (block.Validated == false) {
		BlockChain[Hash(tailHash)] = Block{
			PrevHash:			block.PrevHash,
			Client:             block.Client,
			LocalTransactionID: block.LocalTransactionID,
			Transaction:        block.Transaction,
			NodeID:             block.NodeID,
			ValidateNum:        block.ValidateNum,
			Validated:			true,
			Children:           block.Children,
			ChainLen:			block.ChainLen,
			Nonce:              block.Nonce,
		}
		validateRes := ValidateRes{
			TransactionID: block.ChainLen,
			LocalTransactionID: block.LocalTransactionID,
		}

		var reply bool
		if ClientStateTable[block.Client].Alive {
			// debug.Println("tailHash: ", tailHash)
			// debug.Println("numBlocksAfterTail: ", numBlocksAfterTail)
			// blockHash := block.PrevHash
			// debug.Println("blockHash: ", blockHash)
			// blockClient := block.Client
			// debug.Println("blockClient: ", blockClient)
			// rpcClient := ClientStateTable[block.Client].Client
			// debug.Println("rpcClient: ", rpcClient)
			_ = ClientStateTable[block.Client].Client.Call("NodeToClient.BlockValidated", validateRes, &reply)
		}
	}
	if block.ChainLen > 1 {
		validateBlocksOnBlockChain(string(block.PrevHash), numBlocksAfterTail + 1)
	}
}