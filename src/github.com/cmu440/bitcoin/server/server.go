package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"log"
	"os"
	"strconv"
)

/*
 * scheduling policy:
 * Every time the server receives a request from a client.
 * The request will be split into several fixed size blocks of i.e. 1000
 * and then put these blocks into a job queue. When a miner becomes available
 * or a new miner join in, the miner will go to job queue to check whether
 * there is a job waiting for executing. If so, the miner will take the first
 * job out from the queue and execute it. So, we can ensure all the miners
 * always bear the same workload.
 *
 * The reason for choosing 1000 as the fixed size is because a typical Andrew
 * Linux machine can compute SHA-256 hashes at a rate of around 10,000 per
 * second. Thus, it is reasonable for a miner to do 1000 hashes compute at a time.
 */

const (
	jobSize = uint64(1000)
)

type clientInfo struct {
	requestClient *bitcoin.Message
	result        map[uint64]bool
	hash          uint64
	nonce         uint64
	maxnonce      uint64
}

type server struct {
	lspServer  lsp.Server
	clientMap  map[int]*clientInfo
	minerMap   map[int]*job
	jobList    *list.List
	vacantList *list.List
}

type job struct {
	clientID int
	message  *bitcoin.Message
}

func startServer(port int) (*server, error) {
	lserver, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	s := &server{
		lspServer:  lserver,
		clientMap:  make(map[int]*clientInfo),
		minerMap:   make(map[int]*job),
		jobList:    list.New(),
		vacantList: list.New(),
	}

	return s, nil
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "log.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	server, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)
	LOGF.Println("Server listening on port", port)

	defer server.lspServer.Close()

	for {
		ConnID, payload, err := server.lspServer.Read()
		if err != nil {
			// if the read() returned error
			// 1.C-S error
			// 2.M-S error
			if ConnID == 0 {
				LOGF.Println("Server closed!")
				return
			}
			_, isClient := server.clientMap[ConnID]
			job, isMiner := server.minerMap[ConnID]
			if isClient {
				LOGF.Println("Server received error from client")
				delete(server.clientMap, ConnID)
			} else if isMiner {
				fmt.Println("Server received error from miner")
				if job.clientID != 0 {
					server.jobList.PushFront(job)
				}
				delete(server.minerMap, ConnID)
			}

		} else {
			// decide the new message is from client/miner
			var message bitcoin.Message
			err := json.Unmarshal(payload[:len(payload)], &message)
			if err != nil {
				fmt.Println("Error in Unmarshaling:", err)
				continue
			}
			switch message.Type {
			// Join/Result : miner
			// Request: client
			case bitcoin.Join:
				// add to map, then add to vacant list
				_, exist := server.minerMap[ConnID]
				if exist {
					continue
				}
				server.minerMap[ConnID] = &job{
					clientID: 0,
				}
				cleanMinerStatus(server, ConnID)
				LOGF.Printf("Miner:%v joined\n", ConnID)
			case bitcoin.Request:
				_, exist := server.clientMap[ConnID]
				if exist {
					continue
				}
				server.clientMap[ConnID] = &clientInfo{
					requestClient: &message,
					result:        make(map[uint64]bool),
					hash:          ^uint64(0),
					maxnonce:      message.Upper,
				}
				// split job
				length := message.Upper + 1
				low := uint64(0)
				for length > 0 {
					interval := jobSize
					if length < jobSize {
						interval = length
					}
					server.jobList.PushBack(&job{
						clientID: ConnID,
						message:  bitcoin.NewRequest(message.Data, low, low+interval-1),
					})
					low = low + interval
					length = length - interval
				}
			case bitcoin.Result:
				// miner returned the result and put into the client info map
				// if the map is full then we can write result to client
				// 1.mark the task map in the client to be finished
				// 2.renew the hash and nonce value
				// 3.if the map is full then write result to client
				clientID := server.minerMap[ConnID].clientID
				_, exist := server.clientMap[clientID]
				if !exist {
					cleanMinerStatus(server, ConnID)
					continue
				}
				for i := server.minerMap[ConnID].message.Lower; i <= server.minerMap[ConnID].message.Upper; i++ {
					server.clientMap[clientID].result[i] = true
				}
				// renew hash/nonce
				if message.Hash < server.clientMap[clientID].hash {
					server.clientMap[clientID].hash = message.Hash
					server.clientMap[clientID].nonce = message.Nonce
				}
				// write back if map full
				if uint64(len(server.clientMap[clientID].result)) == server.clientMap[clientID].maxnonce+1 {
					writeResultToClient(server, clientID)
				}
				cleanMinerStatus(server, ConnID)
			}
		}
		// in either case, we should check the job list and the result list
		// to see if we could assign new job to a miner
		assignJobToMiner(server)
	}
}

/*
 * writeResultToClient:
 * server writes an Result Type message back to client
 */
func writeResultToClient(server *server, clientID int) {
	fmt.Printf("Server begins to write\n")
	hash := server.clientMap[clientID].hash
	nonce := server.clientMap[clientID].nonce
	message := bitcoin.NewResult(hash, nonce)
	databuf, err := json.Marshal(message)
	if err != nil {
		fmt.Println("Error in Marshaling when writing to client: ", err)
	}
	server.lspServer.Write(clientID, databuf)
	LOGF.Printf("Write hash:%v, nonce:%v to client:%v\n", hash, nonce, clientID)
	// drop client from the list
	delete(server.clientMap, clientID)
}

/*
 * cleanMinerStatus:
 * server clear the status of a miner to be able to accept new job
 */
func cleanMinerStatus(server *server, minerID int) {
	server.minerMap[minerID].clientID = 0
	server.vacantList.PushBack(minerID)
}

/*
 * assignJobToMiner:
 * assign a current job to a vacant miner with the lsp Write() function
 */
func assignJobToMiner(server *server) {
	for server.vacantList.Len() != 0 && server.jobList.Len() != 0 {
		job := server.jobList.Remove(server.jobList.Front()).(*job)
		// check if the client related to the job has died
		// if so, start another iteration
		_, exist := server.clientMap[job.clientID]
		if !exist {
			continue
		}
		minerID := server.vacantList.Remove(server.vacantList.Front()).(int)
		server.minerMap[minerID] = job
		databuf, err := json.Marshal(job.message)
		if err != nil {
			fmt.Println("Error in Marshaling when writing to miner: ", err)
		}
		err = server.lspServer.Write(minerID, databuf)
		if err != nil {
			delete(server.minerMap, minerID)
		}
	}
}
