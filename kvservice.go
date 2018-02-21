/*

 This package specifies the application's interface to the key-value
 service library to be used in assignment 7 of UBC CS 416 2016 W2.

*/

package kvservice

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sort"
	"net/http"
	"io/ioutil"
	"encoding/json"
)

type IpInfo struct {
	Ip       string
	Hostname string
	City     string
	Region   string
	Country  string
	Loc      string
	Org      string
	Postal   string
}

// Represents a key in the system.
type Key string

// Represent a value in the system.
type Value string

var TX *mytx
var TempTXID = 0

// Node server type (for RPC)
type NodeToClient int

type ValidateRes struct {
	TransactionID int
	LocalTransactionID int
}

func (c *NodeToClient) CheckAlive(req string, reply *bool) error {
	// fmt.Println("CheckAlive Request Received from node: ", req)
	*reply = true
	return nil
}

func (c *NodeToClient) ProofOfWork(req string, reply *bool) error {
	fmt.Println("ProofOfWork Request Received from a node")

	if !TX.WorkDone {
		TX.WorkDone = true
		TX.ProofOfWorkChannel <- true
		*reply = true
		return nil
	} else {
		*reply = false
		return nil
	}
}

func (c *NodeToClient) BlockValidated(res ValidateRes, reply *bool) error {
	// fmt.Println("BlockValidated Request Received for txID: ", res.TransactionID)
	
	if TX.LocalTransactionID == res.LocalTransactionID {
		TX.TransactionID = res.TransactionID
		TX.ValidatedChannel <- true
	}

	*reply = true
	return nil
}

func (c *NodeToClient) Abort(localTransactionID string, reply *bool) error {
	fmt.Printf("The node tells me to abort\n")

	if TX.Aborted {
		*reply = true
		return nil
	}

	txID, _ := strconv.Atoi(localTransactionID)
	if !TX.Aborted && TX.LocalTransactionID == txID {
		TX.Aborted = true
		if TX.Commited {
			TX.ProofOfWorkChannel <- true
			TX.ValidatedChannel <- true
		}
	}
	*reply = true
	return nil
	
}

// An interface representing a connection to the key-value store. To
// create a new connection use the NewConnection() method.
type connection interface {
	// The 'constructor' for a new logical transaction object. This is the
	// only way to create a new transaction. The returned transaction must
	// correspond to a specific, reachable, node in the k-v service. If
	// none of the nodes are reachable then tx must be nil and error must
	// be set (non-nil).
	NewTX() (newTX tx, err error)

	// Used by a client to ask a node for information about the
	// block-chain. Node is an IP:port string of one of the nodes that
	// was used to create the connection.  parentHash is either an
	// empty string to indicate that the client wants to retrieve the
	// SHA 256 hash of the genesis block. Or, parentHash is a string
	// identifying the hexadecimal SHA 256 hash of one of the blocks
	// in the block-chain. In this case the return value should be the
	// string representations of SHA 256 hash values of all of the
	// children blocks that have the block identified by parentHash as
	// their prev-hash value.
	GetChildren(node string, parentHash string) (children []string)

	// Close the connection.
	Close()
}

// An interface representing a client's transaction. To create a new
// transaction use the connection.NewTX() method.
type tx interface {
	// Retrieves a value v associated with a key k as part of this
	// transaction. If success is true then v contains the value
	// associated with k and err is nil. If success is false then the
	// tx has aborted, v is an empty string, and err is non-nil. If
	// success is false, then all future calls on this transaction
	// must immediately return success = false (indicating an earlier
	// abort).
	Get(k Key) (success bool, v Value, err error)

	// Associates a value v with a key k as part of this
	// transaction. If success is true, then put was recoded
	// successfully, otherwise the transaction has aborted (see
	// above).
	Put(k Key, v Value) (success bool, err error)

	// Commits this transaction. If success is true then commit
	// succeeded, otherwise the transaction has aborted (see above).
	// The validateNum argument indicates the number of blocks that
	// must follow this transaction's block in the block-chain along
	// the longest path before the commit returns with a success.
	// txID represents the transactions's global sequence number
	// (which determines this transaction's position in the serialized
	// sequence of all the other transactions executed by the
	// service).
	Commit(validateNum int) (success bool, txID int, err error)

	// Aborts this transaction. This call always succeeds.
	Abort()
}

// The 'constructor' for a new logical connection object. This is the
// only way to create a new connection. Takes a set of k-v service
// node ip:port strings.
func NewConnection(nodes []string) connection {
	fmt.Printf("NewConnection\n")

	var sortedNodes []string

	c := new(myconn)
	c.rpcClients = make(map[string]*rpc.Client)
	for _, nodeIP := range nodes {
		client, err := rpc.Dial("tcp", nodeIP)
		// checkError("", err, true)
		if err == nil {
			// fmt.Println("client: ", client)
			// fmt.Println("clients: ", c.rpcClients)
			// c.rpcClients = append(c.rpcClients, client)
			c.rpcClients[nodeIP] = client
			sortedNodes = append(sortedNodes, nodeIP)
		}
	}

	localListenAddress, err := net.InterfaceAddrs()
	checkError("Cannot get local IP address", err, true)

	// sort nodes list
	sort.Strings(sortedNodes)
	c.nodes = sortedNodes
	fmt.Println("Sorted nodes: ", sortedNodes)

	ipFound := false
	for _, a := range localListenAddress {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipFound = true
				c.ListenAddress = ipnet.IP.String() + ":0"
				// fmt.Println("ListenAddress set as: ", c.ListenAddress)
				// TESTING on windows
				// c.ListenAddress = "127.0.0.1" + ":0"
				// fmt.Println("NOW ListenAddress set as: ", c.ListenAddress)
			}
		}
	}
	if ipFound != true {
		err = fmt.Errorf("Unable to set client IP and Port for listening")
		panic(err)
	}

	// go func() {
	cServer := rpc.NewServer()
	nc := new(NodeToClient)
	cServer.Register(nc)

	l, err := net.Listen("tcp", c.ListenAddress)
	checkError("", err, true)

	fmt.Println("listen on: ", l.Addr().String())
	c.ListenAddress = l.Addr().String()

	// need to get public port
	_, port, _ := net.SplitHostPort(c.ListenAddress)
	host := getPublicIp()
	c.ListenAddress = net.JoinHostPort(host, port)
	fmt.Println("ListenAddress set as: ", c.ListenAddress)

	var success bool
	for _, client := range c.rpcClients {
		_ = client.Call("ClientToNode.ClientListenAddress", c.ListenAddress, &success)
	}

	go func() {
		for {
			conn, err := l.Accept()
			checkError("", err, false)
			go cServer.ServeConn(conn)
		}
	}()

	return c
}

type myconn struct {
	rpcClients    map[string]*rpc.Client
	ListenAddress string
	nodes		[]string //sorted nodes
}

type mytx struct {
	Conn               *myconn
	Aborted            bool
	Commited			bool
	LocalTransactionID int
	TransactionID int
	ProofOfWorkChannel chan bool
	WorkDone           bool
	ValidatedChannel   chan bool
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

// Create a new transaction.
func (conn *myconn) NewTX() (tx, error) {
	TempTXID++
	fmt.Printf("NewTX\n")
	m := new(mytx)
	m.Conn = conn
	m.Aborted = false
	m.Commited = false
	m.LocalTransactionID = TempTXID
	m.ProofOfWorkChannel = make(chan bool)
	m.WorkDone = false
	m.ValidatedChannel = make(chan bool)
	TX = m

	newTransactionReq := NewTransactionReq{
		Client:             m.Conn.ListenAddress,
		LocalTransactionID: m.LocalTransactionID,
	}

	var success bool
	// send request in order
	for i:=0; i<len(m.Conn.nodes); i++ {
		fmt.Println("Current nodeIP is: ", m.Conn.nodes[i])
		nodeIP := m.Conn.nodes[i]

		client := m.Conn.rpcClients[nodeIP]
		err := client.Call("ClientToNode.ClientNewTransaction", newTransactionReq, &success)
		if err != nil {
			delete(m.Conn.rpcClients, nodeIP)
			m.Conn.nodes = append(m.Conn.nodes[:i], m.Conn.nodes[i+1:]...)
			i--
		}
	}
	// for index, client := range m.Conn.rpcClients {
	// 	err := client.Call("ClientToNode.ClientNewTransaction", newTransactionReq, &success)
	// 	if err != nil {
	// 		delete(m.Conn.rpcClients, index)
	// 	}
	// }

	return m, nil
}

// Close the connection.
func (conn *myconn) Close() {
	fmt.Printf("Close\n")
	for _, client := range conn.rpcClients {
		client.Close()
	}
}

func (conn *myconn) GetChildren(node string, parentHash string) (children []string) {
	fmt.Printf("GetChildren\n")

	// send request in order
	for i:=0; i<len(conn.nodes); i++ {
		// fmt.Println("Current nodeIP is: ", conn.nodes[i])
		nodeIP := conn.nodes[i]
		if nodeIP == node {
			client := conn.rpcClients[nodeIP]
			err := client.Call("ClientToNode.GetChildren", parentHash, &children)
			if err != nil {
				delete(conn.rpcClients, nodeIP)
				fmt.Printf("Node cannot be reached\n")
				conn.nodes = append(conn.nodes[:i], conn.nodes[i+1:]...)
				i--
			}
		}
	}
	// for index, client := range conn.rpcClients {
	// 	if index == node {
	// 		err := client.Call("ClientToNode.GetChildren", parentHash, &children)
	// 		if err != nil {
	// 			delete(conn.rpcClients, index)
	// 			fmt.Printf("Node cannot be reached\n")
	// 		}
	// 	}
	// }
	return children
}

type GetReq struct {
	Client string
	Key    Key
}

type GetRes struct {
	Value Value
}

type PutReq struct {
	Client string
	Key    Key
	Value  Value
}

type StartWorkReq struct {
	Client      string
	ValidateNum int
}

func (t *mytx) Get(k Key) (success bool, v Value, err error) {
	fmt.Printf("Get\n")
	if t.Aborted {
		return false, v, errors.New("transaction already aborted")
	}

	getReq := GetReq{
		Client: t.Conn.ListenAddress,
		Key:    k,
	}

	var getRes GetRes

	// send request in order
	for i:=0; i<len(t.Conn.nodes); i++ {
		// fmt.Println("Current nodeIP is: ", t.Conn.nodes[i])
		nodeIP := t.Conn.nodes[i]

		client := t.Conn.rpcClients[nodeIP]
		err = client.Call("ClientToNode.Get", getReq, &getRes)
		if err != nil {
			success = success || false
			delete(t.Conn.rpcClients, nodeIP)
			t.Conn.nodes = append(t.Conn.nodes[:i], t.Conn.nodes[i+1:]...)
			i--
		} else {
			success = success || true
			v = getRes.Value
		}
	}
	// for index, client := range t.Conn.rpcClients { //TODO: test node failure while sending puts
	// 	err = client.Call("ClientToNode.Get", getReq, &getRes)
	// 	if err != nil {
	// 		success = success || false
	// 		delete(t.Conn.rpcClients, index)
	// 	} else {
	// 		success = success || true
	// 		v = getRes.Value
	// 	}
	// }

	return success, v, err
}

func (t *mytx) Put(k Key, v Value) (success bool, err error) {
	fmt.Printf("Put\n")
	if t.Aborted {
		return false, errors.New("transaction already aborted")
	}

	putReq := PutReq{
		Client: t.Conn.ListenAddress,
		Key:    k,
		Value:  v,
	}

	// send request in order
	for i:=0; i<len(t.Conn.nodes); i++ {
		// fmt.Println("Current nodeIP is: ", t.Conn.nodes[i])
		nodeIP := t.Conn.nodes[i]

		client := t.Conn.rpcClients[nodeIP]
		err = client.Call("ClientToNode.Put", putReq, &success)
		if err != nil {
			delete(t.Conn.rpcClients, nodeIP)
			t.Conn.nodes = append(t.Conn.nodes[:i], t.Conn.nodes[i+1:]...)
			i--
		}
	}

	// for index, client := range t.Conn.rpcClients { //TODO: test node failure while sending puts
	// 	err = client.Call("ClientToNode.Put", putReq, &success)
	// 	if err != nil {
	// 		delete(t.Conn.rpcClients, index)
	// 	}
	// }

	return success, err
}

func (t *mytx) Commit(validateNum int) (success bool, txID int, err error) {
	t.Commited = true
	fmt.Printf("Commit\n")
	if t.Aborted {
		return false, 0, errors.New("transaction already aborted")
	}

	startWorkReq := StartWorkReq{
		Client:      t.Conn.ListenAddress,
		ValidateNum: validateNum,
	}

	// send request in order
	for i:=0; i<len(t.Conn.nodes); i++ {
		// fmt.Println("Current nodeIP is: ", t.Conn.nodes[i])
		nodeIP := t.Conn.nodes[i]

		client := t.Conn.rpcClients[nodeIP]
		err = client.Call("ClientToNode.StartWork", startWorkReq, &success)
		if err != nil {
			delete(t.Conn.rpcClients, nodeIP)
			t.Conn.nodes = append(t.Conn.nodes[:i], t.Conn.nodes[i+1:]...)
			i--
		}
	}
	
	// for index, client := range t.Conn.rpcClients {
	// 	err = client.Call("ClientToNode.StartWork", startWorkReq, &success)
	// 	if err != nil {
	// 		delete(t.Conn.rpcClients, index)
	// 	}
	// }

	<-t.ProofOfWorkChannel //wait for the proof of works to come in

	<-t.ValidatedChannel //wait for the validation to come in

	if !t.Aborted {
		return true, t.TransactionID, nil
	} else {
		return false, 0, errors.New("transaction aborted")
	}
	
}

func (t *mytx) Abort() {
	fmt.Printf("Abort\n")

	if t.Aborted {
		return
	}

	var reply bool

	// send request in order
	for i:=0; i<len(t.Conn.nodes); i++ {
		// fmt.Println("Current nodeIP is: ", t.Conn.nodes[i])
		nodeIP := t.Conn.nodes[i]

		client := t.Conn.rpcClients[nodeIP]
		_ = client.Call("ClientToNode.Abort", t.Conn.ListenAddress, &reply)
	}
	// for _, client := range t.Conn.rpcClients {
	// 	_ = client.Call("ClientToNode.Abort", t.Conn.ListenAddress, &reply)
	// }

	t.Aborted = true
}

func checkError(msg string, err error, exit bool) {
	if err != nil {
		log.Println(msg, err)
		if exit {
			os.Exit(-1)
		}
	}
}

func getPublicIp() string {
	res, err := http.Get("http://ipinfo.io/json")
	if err != nil {
		log.Fatalf("Failed to get public ip info %v; try again please \n", err)
	}
	defer res.Body.Close()
	content, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()

	var ipInfo IpInfo
	err = json.Unmarshal(content, &ipInfo)
	if err != nil {
		log.Fatal(err)
	}
	return ipInfo.Ip
}