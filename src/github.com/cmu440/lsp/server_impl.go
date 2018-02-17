// Contains the implementation of a LSP server.
/* Team info
 * name: Liang Rao, ID: lrao
 * name: Qingyu Yang, ID: qingyuy
 */
package lsp

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

const (
	defaultBufferSize = 1500
	verbose           = false
)

/*
 * info:
 * tracks the client info(addr) and the message sent to the server
 */
type info struct {
	cliAddr *lspnet.UDPAddr
	n       int
	buff    []byte
}

type shortMessageType struct {
	ConnID  int
	Payload []byte
}

type writeBackMessage struct {
	ConnID  int
	SeqNum  int
	cliAddr *lspnet.UDPAddr
	Payload []byte
}

/*
 * longMessageType:
 * if a message has been sent to the client, this type of information would be
 * responsible for tracking epoch resending info (current interval,...)
 */
type longMessageType struct {
	message     *Message
	epochToSent int
	curInterval int
}

/*
 * clientTrackingMap:
 * a structure to track all client related information.
 * - epoch denotes the current epoch client is in
 * - currentEpochAction is to mark if anything(data/ack) has been sent to client
 * - waitingForAckMessage maintains a map for epoch resending, for each epoch, we resend
 *   data message if the value reaches 0, and subtract the value by 1
 */
type clientTrackingMap struct {
	clientSequenceMap    int
	clientAddressMap     *lspnet.UDPAddr
	clientBufferQueue    *list.List
	writeSequenceMap     int
	writeBufferQueue     *list.List
	ackSequenceMap       int
	ackClientQueue       *list.List
	epoch                int
	currentEpochAction   bool
	waitingForAckMessage map[int]*longMessageType
	clientMessageSet     map[int]bool
	isCalledClose        bool
}

type server struct {
	// TODO: implement this!
	udpConn                   *lspnet.UDPConn
	requestQueue              chan *info
	readResult                chan *Message
	nextClientId              int
	params                    *Params
	readRequest               chan int
	readBlock                 chan int
	closeRequest              chan int
	readClientLost            chan int
	closeBlock                chan int
	closeClientConnectRequest chan int
	closeAcptReadRoutine      chan int
	closeReadRoutine          chan int
	closeEpochFile            chan int
	writeTaskQueue            chan *shortMessageType
	writeResult               chan bool
	clientMap                 map[int]*clientTrackingMap
	readyToWriteTask          chan *writeBackMessage
	readStorage               *list.List
	writeStorage              *list.List
	writeTask                 chan int
	seq                       int
	clientAddrSet             map[lspnet.UDPAddr]int
	ticker                    *time.Ticker
	epochRenew                chan int
	clientCloseError          chan bool
	isCalledClose             bool
	serverClosed              chan int
	serverAlreadyClosed       bool
	lostClient                *list.List
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", ":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println("Error on ResolveUDPAddr")
		return nil, err
	}
	udpConn, err2 := lspnet.ListenUDP("udp", addr)
	if err2 != nil {
		fmt.Println("Error in ListenUDP")
		return nil, err2
	}

	s := &server{
		udpConn:                   udpConn,
		requestQueue:              make(chan *info),
		nextClientId:              1,
		params:                    params,
		readResult:                make(chan *Message),
		readRequest:               make(chan int),
		readBlock:                 make(chan int),
		readClientLost:            make(chan int),
		closeRequest:              make(chan int),
		closeBlock:                make(chan int),
		closeClientConnectRequest: make(chan int),
		closeAcptReadRoutine:      make(chan int),
		closeReadRoutine:          make(chan int),
		closeEpochFile:            make(chan int),
		writeTaskQueue:            make(chan *shortMessageType),
		writeResult:               make(chan bool),
		clientMap:                 make(map[int]*clientTrackingMap),
		readyToWriteTask:          make(chan *writeBackMessage),
		readStorage:               list.New(),
		writeStorage:              list.New(),
		writeTask:                 make(chan int),
		seq:                       1,
		clientAddrSet:             make(map[lspnet.UDPAddr]int),
		ticker:                    time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond),
		epochRenew:                make(chan int),
		clientCloseError:          make(chan bool),
		isCalledClose:             false,
		serverClosed:              make(chan int),
		serverAlreadyClosed:       false,
		lostClient:                list.New(),
	}
	// accepting clients && read
	go acceptAndRead(s)

	// Main server handling routine
	go mainRoutine(s)

	// A routine for epoch ticker
	go epochFire(s)

	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	if verbose {
		fmt.Println("                                Read() called")
	}
	s.readRequest <- 1
	//fmt.Println("                                Read() called2")
	for {
		select {
		case <-s.readBlock:
			time.Sleep(1 * time.Microsecond)
			s.readRequest <- 1
		case <-s.closeReadRoutine:
			return 0, nil, errors.New("Server Closed, read fail")
		case id := <-s.readClientLost:
			if verbose {
				fmt.Printf("                                ERROR from client:%v\n", id)
			}
			return id, nil, errors.New("Client Disconnected, read fail")
		case message := <-s.readResult:
			// send request to the server(main routine) to get message
			// to check if the client has disconnected
			// send request to server whether the client has disconnedted
			if verbose {
				fmt.Printf("                                Read from client:%v, seq:%v\n", message.ConnID, message.SeqNum)
			}
			return message.ConnID, message.Payload, nil
		}
	}
}

func (s *server) Write(connID int, payload []byte) error {
	if verbose {
		fmt.Printf("                                Write() called, %v\n", s.seq)
	}
	s.writeTaskQueue <- &shortMessageType{
		ConnID:  connID,
		Payload: payload,
	}
	res := <-s.writeResult
	if res {
		return nil
	}
	return errors.New("Client Disconnected, write fail")
}

func (s *server) CloseConn(connID int) error {
	if verbose {
		fmt.Printf("                                closeconn %v called\n", connID)
	}
	s.closeClientConnectRequest <- connID
	result := <-s.clientCloseError
	if result {
		return errors.New("CloseConn called on non-existing client")
	}
	return nil
}

func (s *server) Close() error {
	if verbose {
		fmt.Printf("                                Close()\n")
	}
	s.closeRequest <- 1
	for {
		select {
		case <-s.closeBlock:
			time.Sleep(1 * time.Microsecond)
			s.closeRequest <- 1
		case <-s.serverClosed:
			return nil
		}
	}
}

func acceptAndRead(s *server) {
	for {
		select {
		case <-s.closeAcptReadRoutine:
			if verbose {
				fmt.Printf("                                Acpt chan closed by signal!\n")
			}
			return
		default:
			b := make([]byte, 1500)
			n, cliAddr, err := s.udpConn.ReadFromUDP(b)
			if err != nil {
				if verbose {
					fmt.Printf("                                Error in reading from clients!\n")
				}
				continue
			}
			s.requestQueue <- &info{
				cliAddr: cliAddr,
				n:       n,
				buff:    b,
			}
		}
	}
}

func mainRoutine(s *server) {
	for {
		select {
		case <-s.epochRenew:
			// plus all the client epoch count by 1
			// if reaches the max epochs, then declare connection lost
			// then for every client, we check the waitingForAckMessage to decide if any message need to be sent
			lostSet := make(map[int]bool)
			for k, v := range s.clientMap {
				v.epoch = v.epoch + 1
				if v.epoch >= s.params.EpochLimit {
					lostSet[k] = true
					continue
				}
				checkAndResend(s, k)
				// set the currentEpochAction to false
				s.clientMap[k].currentEpochAction = false
			}
			// drop lost connections
			for lost, _ := range lostSet {
				// if client connection lost, put and error message to the back of the list
				if verbose {
					fmt.Printf("                                client:%v connection lost!\n", lost)
				}
				delete(s.clientAddrSet, *s.clientMap[lost].clientAddressMap)
				delete(s.clientMap, lost)
				// s.lostClient.PushBack(lost)
				m := &Message{
					ConnID: lost,
					SeqNum: -1, // mark the client as lost
				}
				s.readStorage.PushBack(*m)
			}
		case newinfo := <-s.requestQueue:
			//fmt.Println("request received")
			// process request (connect/data/ack)
			var messageList Message
			err := json.Unmarshal(newinfo.buff[:newinfo.n], &messageList)
			if err != nil {
				fmt.Println("Error in Unmarshaling:", err)
				continue
			}
			message := messageList
			switch message.Type {
			case MsgConnect: // new connection arrived
				// when a new connection request arrives, there are 2 conditions:
				// 1.The client info is already in the server (may caused by server ack drop)
				// 2.The client is new to the server
				clientID, exist := s.clientAddrSet[*newinfo.cliAddr]
				if exist {
					// case 1
					// ack back
					sendAckToClient(s, clientID, 0, newinfo.cliAddr)
					// renew epoch
					s.clientMap[clientID].epoch = 0
					s.clientMap[clientID].currentEpochAction = true
				} else {
					// case 2
					s.clientAddrSet[*newinfo.cliAddr] = s.nextClientId
					s.clientMap[s.nextClientId] = &clientTrackingMap{
						// renew the next sequence number
						clientSequenceMap: 1,
						// grant new connection id with addr
						clientAddressMap: newinfo.cliAddr,
						// create client buffer queue for storing arrived packets and maintain order
						clientBufferQueue: list.New(),
						// set the writeSequenceMap & writeBufferQueue
						writeSequenceMap: 1,
						writeBufferQueue: list.New(),
						// set the client-related ackSequenceMap & ackClientQueue
						ackSequenceMap: 0,
						ackClientQueue: list.New(),
						// set client epoch count
						epoch:                0,
						currentEpochAction:   true,
						waitingForAckMessage: make(map[int]*longMessageType),
						clientMessageSet:     make(map[int]bool),
						isCalledClose:        false,
					}
					//fmt.Printf("connection for client %v\n", s.nextClientId)
					// ack back
					sendAckToClient(s, s.nextClientId, 0, newinfo.cliAddr)
					s.nextClientId++
				}

			case MsgData: // new client data arrived
				/* when a new data for a client arrived, there are 3 conditions:
				 * 1. data seq < target: may caused by server ack drop (leave it)
				 * 2. data seq = target: (put into readStorage)
				 * 3. data seq > target: (put into client buffer and keep order)
				 * No matter which case, we should renew the epoch
				 * case 2 and 3 may also contains the resend condition
				 */
				// if the client has already been dropped, just ignore the message
				_, present := s.clientMap[message.ConnID]
				if !present {
					continue
				}
				if verbose {
					fmt.Printf("                                received data from client:%v, seq:%v\n", message.ConnID, message.SeqNum)
				}
				// renew epoch
				s.clientMap[message.ConnID].epoch = 0
				// if client data size > length, simply drop the message without any server actions
				if message.Size > len(message.Payload) {
					continue
				}
				// if client data size < length, we cut the payload to fit the length
				if message.Size < len(message.Payload) {
					message.Payload = message.Payload[0:message.Size]
				}
				// deal with data according to seq
				_, exist := s.clientMap[message.ConnID].clientMessageSet[message.SeqNum]
				if exist {
					if verbose {
						fmt.Printf("                                ACK for client %v, seq %v\n", message.ConnID, message.SeqNum)
					}
					sendAckToClient(s, message.ConnID, message.SeqNum, newinfo.cliAddr)
					s.clientMap[message.ConnID].currentEpochAction = true
					continue
				}
				s.clientMap[message.ConnID].clientMessageSet[message.SeqNum] = true
				if s.clientMap[message.ConnID].clientSequenceMap == message.SeqNum {
					s.readStorage.PushBack(message)
					s.clientMap[message.ConnID].clientSequenceMap = s.clientMap[message.ConnID].clientSequenceMap + 1
				} else if s.clientMap[message.ConnID].clientSequenceMap < message.SeqNum {
					// if meg seq is not seqmap, then put into the client buffer
					// and keep the order
					putNewMessageIntoClientBuffer(s, message)
				}
				putBufferMessageIntoReadStorage(s, message.ConnID)

				if verbose {
					fmt.Printf("                                ACK for client %v, seq %v\n", message.ConnID, message.SeqNum)
				}
				// ack back
				sendAckToClient(s, message.ConnID, message.SeqNum, newinfo.cliAddr)
				s.clientMap[message.ConnID].currentEpochAction = true
			case MsgAck: // new ack from Write() arrived
				/* when a new ack from a client arrives, there are 3 conditions:
				 * 1. ack seq < target: especially the ack(0) for maintaining connection
				 * 2. ack seq = target: (slide window)
				 * 3. ack seq > target: (put into ack buffer)
				 * No matter which case, we should renew the epoch
				 * For 2 and 3, we should drop the message from waiting map since we received ack
				 */
				// if the client has already been dropped, just ignore the message
				_, present := s.clientMap[message.ConnID]
				if !present {
					continue
				}
				if verbose {
					fmt.Printf("                                ack from client:%v, seq:%v\n", message.ConnID, message.SeqNum)
				}
				s.clientMap[message.ConnID].epoch = 0
				if verbose {
					fmt.Printf("                                current seq:%v\n", message.SeqNum)
					fmt.Printf("                                target seq:%v\n", s.clientMap[message.ConnID].ackSequenceMap+1)
				}
				if s.clientMap[message.ConnID].ackSequenceMap+1 <= message.SeqNum {
					// drop message in the waiting map
					delete(s.clientMap[message.ConnID].waitingForAckMessage, message.SeqNum)
					// if the new ack is the target one, then slide window
					if s.clientMap[message.ConnID].ackSequenceMap+1 == message.SeqNum {
						s.clientMap[message.ConnID].ackSequenceMap = s.clientMap[message.ConnID].ackSequenceMap + 1
					} else {
						// if the new ack is not the target one, then put into ack buffer
						putNewAckIntoBuffer(s, message)
					}
					if s.clientMap[message.ConnID].ackClientQueue.Len() > 0 {
						if verbose {
							fmt.Println("                                processing ACK buffer")
						}
						processAckBuffer(s, message.ConnID)
					}
					// deal with the writeBufferQueue
					for s.clientMap[message.ConnID].writeBufferQueue.Len() > 0 && s.clientMap[message.ConnID].writeSequenceMap <= s.params.WindowSize+s.clientMap[message.ConnID].ackSequenceMap {
						writeElement := s.clientMap[message.ConnID].writeBufferQueue.Front()
						s.clientMap[message.ConnID].writeBufferQueue.Remove(writeElement)
						shortMessage := writeElement.Value.(*shortMessageType)
						writeDataToClient(s, *shortMessage)
					}
					// delete the client if the last written message ack is received
					if (s.clientMap[message.ConnID].isCalledClose || s.isCalledClose) && s.clientMap[message.ConnID].writeSequenceMap == s.clientMap[message.ConnID].ackSequenceMap+1 && s.clientMap[message.ConnID].writeBufferQueue.Len() == 0 && s.clientMap[message.ConnID].ackClientQueue.Len() == 0 {
						if verbose {
							fmt.Printf("                                client:%v deleted by closeconn\n", message.ConnID)
						}
						delete(s.clientAddrSet, *s.clientMap[message.ConnID].clientAddressMap)
						delete(s.clientMap, message.ConnID)
					}
				}
			}
		case <-s.readRequest:
			// server receives a readrequest, put the top of the readstorage list
			// into the channel
			if s.serverAlreadyClosed {
				s.closeReadRoutine <- 1
				if verbose {
					fmt.Printf("                                Server closed!\n")
				}
				return
			}
			if s.readStorage.Len() > 0 {
				if verbose {
					fmt.Printf("                                read reached here! size:%v\n", s.readStorage.Len())
				}
				front := s.readStorage.Front()
				s.readStorage.Remove(front)
				message := front.Value.(Message)
				//_, exist := s.clientMap[message.ConnID]
				if message.SeqNum == -1 {
					s.readClientLost <- message.ConnID
				} else {
					s.readResult <- &message
				}
			} else {
				s.readBlock <- 1
			}
		case m := <-s.writeTaskQueue:
			_, present := s.clientMap[m.ConnID]
			if present {
				// client is connected
				s.writeResult <- true
				// server recives a write request, then check if it can send the package
				// with in window size. If so, send it to client, otherwise store the message
				// Logic:
				// if current seq < w + ackSeq then write directly
				// otherwise put into buffer and wait
				if verbose {
					fmt.Printf("                                next write seq:%v\n", s.clientMap[m.ConnID].writeSequenceMap)
					fmt.Printf("                                cur ack seq:%v\n", s.clientMap[m.ConnID].ackSequenceMap)
				}
				if s.clientMap[m.ConnID].writeSequenceMap <= s.params.WindowSize+s.clientMap[m.ConnID].ackSequenceMap {
					// write to client here
					writeDataToClient(s, *m)
				} else {
					s.clientMap[m.ConnID].writeBufferQueue.PushBack(m)
				}
			} else {
				s.writeResult <- false
			}

		case closeID := <-s.closeClientConnectRequest:
			// if client doesn't exist, return an indicator of error
			_, exist := s.clientMap[closeID]
			if exist {
				s.clientCloseError <- false
				// delete client from the map if buffer is 0
				// otherwise mark the client as deleted till pending message is sent
				s.clientMap[closeID].isCalledClose = true
			} else {
				s.clientCloseError <- true
			}
		case <-s.closeRequest:
			//s.closeReadRoutine <-
			if len(s.clientMap) != 0 {
				s.isCalledClose = true
				s.closeBlock <- 1
			} else { // close all
				if verbose {
					fmt.Printf("                                close Req received in main\n")
				}
				// close routine: epochFire, acceptAndRead, and current routine
				s.udpConn.Close()
				s.closeEpochFile <- 1
				s.closeAcptReadRoutine <- 1
				s.serverClosed <- 1
				s.serverAlreadyClosed = true
			}
		}
	}
}

/*
 * epochFire:
 * set the timer and when an epoch arrives, tick and send the info back
 * through channel
 */
func epochFire(s *server) {
	for {
		select {
		case <-s.closeEpochFile:
			s.ticker.Stop()
			if verbose {
				fmt.Printf("                                epoch closed!\n")
			}
			return
		case <-s.ticker.C:
			s.epochRenew <- 1
		}
	}
}

/*
 * sendAckTo Client:
 * send an ack message to client according to client connection id
 * sequence number and address
 */
func sendAckToClient(s *server, ConnID int, SeqNum int, cliAddr *lspnet.UDPAddr) {
	ackbuf, err := json.Marshal(NewAck(ConnID, SeqNum))
	if err != nil {
		fmt.Println("Error in Marshaling ACK: ", err)
	}
	s.udpConn.WriteToUDP(ackbuf, cliAddr)
}

/*
 * putBufferMessageIntoReadStorage:
 * checking the seq number of one certain client and put the buffered message
 * into the ReadStorage if matched
 */
func putBufferMessageIntoReadStorage(s *server, ConnID int) {
	for s.clientMap[ConnID].clientBufferQueue.Len() > 0 && s.clientMap[ConnID].clientSequenceMap == s.clientMap[ConnID].clientBufferQueue.Front().Value.(Message).SeqNum {
		front := s.clientMap[ConnID].clientBufferQueue.Front()
		s.clientMap[ConnID].clientBufferQueue.Remove(front)
		msg := front.Value.(Message)
		s.readStorage.PushBack(msg)
		s.clientMap[ConnID].clientSequenceMap = s.clientMap[ConnID].clientSequenceMap + 1
	}
}

/*
 * putNewMessageIntoClientBuffer:
 * for an incomming message that doesn't match the client sequence number,
 * use the clientmessagebuffer as a list to temporarily store them
 */
func putNewMessageIntoClientBuffer(s *server, message Message) {
	// if list is empty, just insert
	// if the last one has a seq smaller than the message, insert back
	// then insert in front of the first one bigger than message
	if s.clientMap[message.ConnID].clientBufferQueue.Len() == 0 {
		s.clientMap[message.ConnID].clientBufferQueue.PushBack(message)
	} else if s.clientMap[message.ConnID].clientBufferQueue.Back().Value.(Message).SeqNum < message.SeqNum {
		s.clientMap[message.ConnID].clientBufferQueue.PushBack(message)
	} else {
		for e := s.clientMap[message.ConnID].clientBufferQueue.Front(); e != nil; e = e.Next() {
			if message.SeqNum < e.Value.(Message).SeqNum {
				s.clientMap[message.ConnID].clientBufferQueue.InsertBefore(message, e)
				break
			}
		}
	}
}

/*
 * processAckBuffer:
 * similar to putBufferMessageIntoReadStorage, check the ack seq number
 * and renew the ackbuffer if necessary
 */
func processAckBuffer(s *server, ConnID int) {
	for s.clientMap[ConnID].ackClientQueue.Len() > 0 && s.clientMap[ConnID].ackSequenceMap+1 == s.clientMap[ConnID].ackClientQueue.Front().Value.(int) {
		front := s.clientMap[ConnID].ackClientQueue.Front()
		s.clientMap[ConnID].ackClientQueue.Remove(front)
		s.clientMap[ConnID].ackSequenceMap = s.clientMap[ConnID].ackSequenceMap + 1
		if verbose {
			fmt.Printf("                                new target ack is: %v\n", s.clientMap[ConnID].ackSequenceMap+1)
		}
	}
}

/*
 * putNewAckIntoBuffer:
 * for an incomming ack that doesn't match the client ack sequence number,
 * use the ackClientQueue as a list to temporarily store them
 */
func putNewAckIntoBuffer(s *server, message Message) {
	if s.clientMap[message.ConnID].ackClientQueue.Len() == 0 {
		s.clientMap[message.ConnID].ackClientQueue.PushBack(message.SeqNum)
	} else if s.clientMap[message.ConnID].ackClientQueue.Back().Value.(int) < message.SeqNum {
		s.clientMap[message.ConnID].ackClientQueue.PushBack(message.SeqNum)
	} else {
		for e := s.clientMap[message.ConnID].ackClientQueue.Front(); e != nil; e = e.Next() {
			if message.SeqNum < e.Value.(int) {
				s.clientMap[message.ConnID].ackClientQueue.InsertBefore(message.SeqNum, e)
				break
			}
		}
	}
}

/*
 * writeDataToClient:
 * write a data message to a client according to the lsp address
 */
func writeDataToClient(s *server, m shortMessageType) {
	SeqNum := s.clientMap[m.ConnID].writeSequenceMap
	message := NewData(m.ConnID, SeqNum, len(m.Payload), m.Payload)
	databuf, err := json.Marshal(message)
	if err != nil {
		fmt.Println("Error in Marshaling when writing: ", err)
	}
	// back up message for resending
	s.clientMap[m.ConnID].waitingForAckMessage[SeqNum] = &longMessageType{
		message:     message,
		epochToSent: 0,
		curInterval: 0,
	}
	if verbose {
		fmt.Printf("                                write to client:%v, seq:%v\n", m.ConnID, s.clientMap[m.ConnID].writeSequenceMap)
	}
	s.clientMap[m.ConnID].writeSequenceMap = s.clientMap[m.ConnID].writeSequenceMap + 1
	s.udpConn.WriteToUDP(databuf, s.clientMap[m.ConnID].clientAddressMap)
}

/*
 * checkAndResend:
 * check the waiting messages and propose the epoch resend strategy
 */
func checkAndResend(s *server, ConnID int) {
	// check the waitingForAckMessage to see if there is pending data to resend
	for k, v := range s.clientMap[ConnID].waitingForAckMessage {
		if v.epochToSent == 0 {
			resendDataToClient(s, *s.clientMap[ConnID].waitingForAckMessage[k].message)
			s.clientMap[ConnID].currentEpochAction = true
			// reset the interval
			if s.params.MaxBackOffInterval == 0 {
				s.clientMap[ConnID].waitingForAckMessage[k].epochToSent = 0
			} else {
				cur := s.clientMap[ConnID].waitingForAckMessage[k].curInterval
				if cur == 0 {
					cur++
				} else {
					cur = cur * 2
				}
				if cur > s.params.MaxBackOffInterval {
					cur = s.params.MaxBackOffInterval
				}
				s.clientMap[ConnID].waitingForAckMessage[k].curInterval = cur
			}
		} else {
			s.clientMap[ConnID].waitingForAckMessage[k].epochToSent = s.clientMap[ConnID].waitingForAckMessage[k].epochToSent - 1
		}
	}
	// if currentEpochAction is false, then send ack(0) as required
	if s.clientMap[ConnID].currentEpochAction == false {
		if verbose {
			fmt.Printf("                                ACK0 to client:%v\n", ConnID)
		}
		sendAckToClient(s, ConnID, 0, s.clientMap[ConnID].clientAddressMap)
	}
}

func resendDataToClient(s *server, m Message) {
	databuf, err := json.Marshal(m)
	if err != nil {
		fmt.Println("Error in Marshaling when writing: ", err)
	}
	if verbose {
		fmt.Printf("                                Resending to client:%v message seq:%v\n", m.ConnID, m.SeqNum)
	}
	s.udpConn.WriteToUDP(databuf, s.clientMap[m.ConnID].clientAddressMap)
}
