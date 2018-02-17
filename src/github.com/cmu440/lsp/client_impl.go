// Contains the implementation of a LSP client.
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
	"time"
)

type client struct {
	// TODO: implement this!
	params          *Params
	UDPaddr         *lspnet.UDPAddr
	UDPconn         *lspnet.UDPConn
	connId          int
	blockRead       chan bool
	requestRead     chan bool
	requestWrite    chan bool
	dataMsgCh       chan *Message
	ackMsgCh        chan *Message
	readCh          chan *Message
	writeCh         chan *Mssg
	readBuf         *list.List
	writeBuf        *list.List
	readList        *list.List //main read list
	writeStart      int
	writeErr        chan error
	seqNum          int
	nextRead        int
	isConn          bool
	connIdCh        chan bool
	getConnIdCh     chan int
	requestClose    chan bool
	blockClose      chan bool
	quit            chan bool
	epochCnt        int
	ticker          *time.Ticker
	epochResendCh   chan bool
	isReceiveData   bool
	isTimeout       bool
	errReadCh       chan bool
	isClosed        bool
	severMessageSet map[int]bool
	connACKch       chan Message
	writeLost       chan bool
	hasReadBlock    bool
}

type Mssg struct {
	message    *Message
	curBackOff int
	interval   int
	count      int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	c := client{
		params:          params,
		connId:          0,
		blockRead:       make(chan bool),
		requestRead:     make(chan bool),
		requestWrite:    make(chan bool),
		dataMsgCh:       make(chan *Message),
		ackMsgCh:        make(chan *Message),
		readCh:          make(chan *Message),
		writeCh:         make(chan *Mssg),
		readBuf:         list.New(),
		writeBuf:        list.New(),
		readList:        list.New(),
		seqNum:          0,
		nextRead:        1,
		writeStart:      1,
		isConn:          false, //////
		connIdCh:        make(chan bool),
		getConnIdCh:     make(chan int),
		requestClose:    make(chan bool),
		blockClose:      make(chan bool),
		quit:            make(chan bool),
		epochCnt:        0,
		ticker:          time.NewTicker(time.Duration(params.EpochMillis) * time.Millisecond),
		epochResendCh:   make(chan bool),
		isReceiveData:   false,
		isTimeout:       false,
		errReadCh:       make(chan bool),
		isClosed:        false,
		severMessageSet: make(map[int]bool),
		connACKch:       make(chan Message),
		writeLost:       make(chan bool),
		hasReadBlock:    false}
	err := c.Start(hostport)
	return &c, err
}

func (c *client) Start(hostport string) error {
	var err error
	c.UDPaddr, err = lspnet.ResolveUDPAddr("udp", hostport)

	if err != nil {
		return fmt.Errorf("Error: ", err)
	}

	c.UDPconn, err = lspnet.DialUDP("udp", nil, c.UDPaddr)

	if err != nil {
		return fmt.Errorf("Error: ", err)
	}
	fmt.Println("Connecting to ", hostport)
	go c.handleAction()
	go c.epochFire()

	mssgConn := NewConnect()

	mssgBuf, err_m := json.Marshal(mssgConn)
	if err_m != nil {
		return fmt.Errorf("Error: ", err_m)
	}

	_, err = c.UDPconn.Write(mssgBuf)
	if err != nil {
		return fmt.Errorf("Error: ", err)
	}

	buf := make([]byte, 200)
	mssg := Message{}
	n, err := c.UDPconn.Read(buf)

	if err != nil {
		mssg = <-c.connACKch
	} else {
		json.Unmarshal(buf[0:n], &mssg)
	}

	if mssg.Type == MsgAck {
		c.isConn = true
		c.connId = mssg.ConnID
	} else {
		err = errors.New("Fail to connect with server")
		return fmt.Errorf("Error: ", err)
	}

	go c.mssgListener()
	return nil

}

/*
 * mssgListener: receive data/ack message from server
 */
func (c *client) mssgListener() error {
	for {
		buf := make([]byte, 200)
		n, err := c.UDPconn.Read(buf)
		if err != nil {
			return fmt.Errorf("Error: ", err)
		}
		mssg := Message{}
		json.Unmarshal(buf[0:n], &mssg)
		switch mssg.Type {
		case MsgData:
			c.dataMsgCh <- &mssg
		case MsgAck:
			c.ackMsgCh <- &mssg
		}

	}
	return nil
}

/*
 * handleAction: handle all the operation related to global variable in the main
 *               routine
 */
func (c *client) handleAction() {
	for {
		select {
		//resend connection request/data/ACK0
		case <-c.epochResendCh:

			c.epochCnt++
			if c.epochCnt >= c.params.EpochLimit {
				if c.hasReadBlock {
					c.readCh <- nil
					c.hasReadBlock = false
				}
				c.isTimeout = true
				c.UDPconn.Close()
				c.quit <- true

			} else {
				if !c.isConn {
					mssgBuf, _ := json.Marshal(NewConnect())
					c.UDPconn.Write(mssgBuf)
					buf := make([]byte, 200)
					mssg := Message{}
					n, err := c.UDPconn.Read(buf)
					if err == nil {
						json.Unmarshal(buf[0:n], &mssg)
						if mssg.Type == MsgAck {
							c.connACKch <- mssg
						}

					}
				} else {

					if !c.isReceiveData {
						msg := NewAck(c.connId, 0)
						buf, _ := json.Marshal(msg)
						c.UDPconn.Write(buf)
					}
					c.isReceiveData = false

					mssgTemp := c.writeBuf.Front()

					for i := 0; i < c.params.WindowSize; i++ {
						if mssgTemp == nil {
							break
						}
						if mssgTemp.Value.(*Mssg).message.SeqNum == -1 {
							mssgTemp = mssgTemp.Next()
							continue
						}
						mssgTemp.Value.(*Mssg).count++
						if mssgTemp.Value.(*Mssg).curBackOff == mssgTemp.Value.(*Mssg).count {
							//fmt.Printf("resend data is %d\n", mssgTemp.Value.(*Mssg).message.SeqNum)
							buf, _ := json.Marshal(mssgTemp.Value.(*Mssg).message)
							c.UDPconn.Write(buf)
							if mssgTemp.Value.(*Mssg).interval < c.params.MaxBackOffInterval {
								if mssgTemp.Value.(*Mssg).interval == 0 {
									mssgTemp.Value.(*Mssg).interval = 1
								} else {
									mssgTemp.Value.(*Mssg).interval = 2 * mssgTemp.Value.(*Mssg).interval
								}
							}
							mssgTemp.Value.(*Mssg).curBackOff = mssgTemp.Value.(*Mssg).curBackOff + 1 + mssgTemp.Value.(*Mssg).interval
						}
						mssgTemp = mssgTemp.Next()
					}
				}

			}
			//Get connectionID request
		case <-c.connIdCh:
			c.getConnIdCh <- c.connId
			//deal with data message from server
		case mssg := <-c.dataMsgCh:
			c.epochCnt = 0
			c.isReceiveData = true
			if mssg.Size <= len(mssg.Payload) {
				if mssg.Size < len(mssg.Payload) {
					mssg.Payload = mssg.Payload[0:mssg.Size]
				}
				buf, _ := json.Marshal(NewAck(c.connId, mssg.SeqNum))
				c.UDPconn.Write(buf)
				index := mssg.SeqNum - c.nextRead

				if index >= 0 {
					if c.severMessageSet[mssg.SeqNum] == false {
						c.severMessageSet[mssg.SeqNum] = true
						if mssg.SeqNum == c.nextRead {
							if c.hasReadBlock {
								c.readCh <- mssg
								c.hasReadBlock = false
							} else {
								c.readList.PushBack(mssg)
							}

							mssgTemp := c.readBuf.Front()
							len := c.readBuf.Len()
							for i := 0; i < len; i++ {
								if mssgTemp == nil {
									break
								}
								if mssgTemp.Value.(*Message).SeqNum == c.nextRead+1 {
									c.readList.PushBack(mssgTemp.Value.(*Message))
									c.nextRead += 1
									mssgTemp = mssgTemp.Next()
									c.readBuf.Remove(c.readBuf.Front())
								} else {
									break
								}
							}

							c.nextRead += 1
						} else {
							if c.readBuf.Len() == 0 {
								c.readBuf.PushBack(mssg)

							} else {
								mssgTemp := c.readBuf.Front()
								for i := 0; i < c.readBuf.Len(); i++ {
									if mssgTemp == nil {
										break
									} else {
										if mssgTemp.Value.(*Message).SeqNum > mssg.SeqNum {
											break
										}
										mssgTemp = mssgTemp.Next()
									}
								}

								if mssgTemp == nil {
									c.readBuf.PushBack(mssg)
								} else {
									c.readBuf.InsertBefore(mssg, mssgTemp)
								}

							}

						}

					}
				}

			}
			//deal with ack message from server
		case mssg := <-c.ackMsgCh:
			c.epochCnt = 0
			if mssg.SeqNum != 0 {
				index := mssg.SeqNum - c.writeStart

				if index < c.params.WindowSize && index >= 0 {

					mssgTemp := c.writeBuf.Front()
					for i := 0; i < c.writeBuf.Len(); i++ {
						mssgTemp = mssgTemp.Next()
					}

					mssgTemp = c.writeBuf.Front()
					for i := 0; i < index; i++ {
						mssgTemp = mssgTemp.Next()
					}
					mssgTemp.Value.(*Mssg).message.SeqNum = -1
					mssgTemp = c.writeBuf.Front()
					var i int
					for i = 0; i < c.params.WindowSize; i++ {
						if mssgTemp == nil {
							break
						}
						if mssgTemp.Value.(*Mssg).message.SeqNum != -1 {
							break
						}
						mssgTemp = mssgTemp.Next()
						c.writeBuf.Remove(c.writeBuf.Front())

					}

					c.writeStart += i
					mssgTemp = c.writeBuf.Front()
					for j := 0; j < c.params.WindowSize-i; j++ {
						if mssgTemp == nil {
							break
						}
						mssgTemp = mssgTemp.Next()
					}

					for j := 0; j < i; j++ {
						if mssgTemp == nil {
							break
						}
						newMssg := mssgTemp

						buf, _ := json.Marshal(newMssg.Value.(*Mssg).message)
						c.UDPconn.Write(buf)
						mssgTemp = mssgTemp.Next()
					}
				}
			}
			//deal with read() function message
		case <-c.requestRead:
			if c.isTimeout {
				c.readCh <- nil
				continue
				//return
			}
			// if c.isClosed {
			// 	c.errReadCh <- true
			// 	continue
			// 	//return
			// }

			if c.readList.Len() > 0 {

				msg := c.readList.Front().Value.(*Message)
				c.readList.Remove(c.readList.Front())
				c.readCh <- msg

			} else {
				c.hasReadBlock = true
				// c.blockRead <- true
			}
		case <-c.requestWrite:
			if c.isTimeout {
				c.writeLost <- true
			} else {
				c.writeLost <- false
			}
			//deal with write() function message
		case mssg := <-c.writeCh:
			switch mssg.message.Type {
			case MsgConnect:
				buf, _ := json.Marshal(mssg.message)
				c.UDPconn.Write(buf)
			case MsgData:
				c.writeBuf.PushBack(mssg)
				index := mssg.message.SeqNum - c.writeStart
				if index < c.params.WindowSize {
					buf, _ := json.Marshal(mssg.message)
					c.UDPconn.Write(buf)
				}
			}
			//deal with close() function message
		case <-c.requestClose:
			if c.isTimeout {
				c.blockClose <- false
				c.isClosed = true
				return
			}
			mssgTemp := c.writeBuf.Front()
			block := false
			for i := 0; i < c.writeBuf.Len(); i++ {
				if mssgTemp == nil {
					break
				} else if mssgTemp.Value.(*Mssg).message.SeqNum != -1 {
					block = true
					break
				}
			}
			if block {
				c.blockClose <- true
			} else {
				c.blockClose <- false
				c.UDPconn.Close()
				if c.hasReadBlock {
					c.readCh <- nil
					c.hasReadBlock = false
				}
				c.isClosed = true
				c.quit <- true
				c.isClosed = true
				return
			}
		}
	}
}

/*
 * epochFire: set the timer and when an epoch arrives, tick and send the info back
 *            through channel
 */
func (c *client) epochFire() {
	for {
		select {
		case <-c.quit:
			c.ticker.Stop()
			return
		case <-c.ticker.C:
			c.epochResendCh <- true
		}
	}
}

func (c *client) ConnID() int {
	c.connIdCh <- true
	for {
		select {
		case ans := <-c.getConnIdCh:
			return ans
		}
	}
	return -1
}

func (c *client) Read() ([]byte, error) {
	c.requestRead <- true
	msg := <-c.readCh
	if msg == nil {
		return nil, errors.New("Read error: Connection has been closed or lost")
	} else {
		return msg.Payload, nil
	}

	// for {
	// 	select {
	// 	case <-c.blockRead:
	// 		time.Sleep(10 * time.Millisecond)
	// 		c.requestRead <- true
	// 	case mssg := <-c.readCh:
	// 		return mssg.Payload, nil

	// 	case <-c.errReadCh:
	// 		return nil, errors.New("Read error: Connection has been closed or lost")
	// 	}
	// }

}

func (c *client) Write(payload []byte) error {

	c.requestWrite <- true
	lost := <-c.writeLost
	if lost {
		return errors.New("Write error: Connection has been closed or lost")
	}
	c.seqNum++
	c.writeCh <- &Mssg{NewData(c.connId, c.seqNum, len(payload), payload), 1, 0, 0}
	return nil
}

func (c *client) Close() error {

	for {
		c.requestClose <- true
		block := <-c.blockClose
		if !block {
			break
		}
		time.Sleep(1 * time.Millisecond)

	}
	return nil
}
