package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		return nil, fmt.Errorf("Error: ", err)
	}

	return client, nil
}

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}
	//close the minner at the end
	defer miner.Close()

	mssg := bitcoin.NewJoin()
	buf, err := json.Marshal(mssg)
	if err != nil {
		fmt.Println("Fail to marshal Join message!")
	}
	err = miner.Write(buf)
	if err != nil {
		fmt.Println("Write return error! Lost the connnection!")
		return
	}

	for {
		buf, err = miner.Read()
		if err != nil {
			fmt.Println("Read return error! Lost the connnection!")
			return
		}
		task := bitcoin.Message{}
		err = json.Unmarshal(buf, &task)
		if err != nil {
			fmt.Println("unmarsh error!")
		}

		//get the date from the assigned task
		data := task.Data
		lower := task.Lower
		upper := task.Upper

		var temp, nonce, hash uint64
		hash = 1<<64 - 1
		nonce = 0
		//caculate the result
		for i := lower; i < upper; i++ {
			temp = bitcoin.Hash(data, i)
			if temp < hash {
				hash = temp
				nonce = i
			}
		}

		bufWrite, err_M := json.Marshal(bitcoin.NewResult(hash, nonce))
		if err_M != nil {
			fmt.Println("Marshal result error!")
		}
		//send back the result
		err_w := miner.Write(bufWrite)
		if err_w != nil {
			fmt.Println("connection lost!")
			return
		}

	}
}
