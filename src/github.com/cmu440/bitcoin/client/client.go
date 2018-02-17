package main

import (
	"encoding/json"
	"fmt"
	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
	"os"
	"strconv"
)

func main() {

	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport> <message> <maxNonce>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	message := os.Args[2]
	maxNonce, err := strconv.ParseUint(os.Args[3], 10, 64)

	client, err := lsp.NewClient(hostport, lsp.NewParams())
	if err != nil {
		fmt.Println("Failed to connect to server:", err)
		return
	}

	//close at the very end
	defer client.Close()

	//send request to server
	request := bitcoin.NewRequest(message, 0, maxNonce)
	buf, err := json.Marshal(request)
	if err != nil {
		fmt.Println("Fail to marshall")
	}

	err = client.Write(buf)

	if err != nil {
		printDisconnected()
		return
	}

	buf, err = client.Read()
	if err != nil {
		printDisconnected()
		return
	}

	//get result from server
	result := bitcoin.Message{}
	err = json.Unmarshal(buf, &result)
	printResult(result.Hash, result.Nonce)
}

// printResult prints the final result to stdout.
func printResult(hash, nonce uint64) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
