package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"strconv"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func partitionServerFromInput(inputfile string, scs ServerConfigs) map[int][][]byte {
	f, err := os.Open(inputfile)
	if err != nil {
		log.Fatal(err)
	}

	// todo error prevent close
	defer f.Close()

	var inputs [][]byte
	for {
		buf := make([]byte, 100)
		n, err := f.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
		inputs = append(inputs, buf[0:n])
	}

	n := int(math.Log2(float64(len(scs.Servers)))) //given the assumption the # of servers can only be 2,4,8,16
	sizeOfAByte := 8
	// todo turn into global

	serverMap := make(map[int][][]byte)

	for _, server := range scs.Servers {
		for _, input := range inputs {
			num := int(input[0] >> (sizeOfAByte - n)) //using the right shift operater to get the first n bit
			if num == server.ServerId {
				serverMap[server.ServerId] = append(serverMap[server.ServerId], input)
			}
		}
	}
	return serverMap

}

func handleConnection(conn net.Conn, ch chan<- []byte) {
	var record []byte
	defer conn.Close()
	for {
		for {
			buffer := make([]byte, 101)
			n, err := conn.Read(buffer)
			if err != nil { // read to the end of file
				if len(record) > 0 {
					ch <- record
				}
				return
			}
			record = append(record, buffer[0:n]...)
			if len(record) >= 101 {
				break
			}
		}
		chunk := record[0:101]
		record = record[101:]
		ch <- chunk
	}
}

func listenForClientConnections(write_only_ch chan<- []byte, host string, port string) {
	listener, err := net.Listen("tcp", host+":"+port)
	if err != nil {
		log.Panic(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn, write_only_ch)
	}

}

func sendData(conn net.Conn, send_data [][]byte, ch chan<- bool) {
	defer conn.Close()
	for _, record := range send_data {
		flag := []byte{byte(1)}
		record = append(flag, record...)
		conn.Write(record)
	}

	end_record := make([]byte, 101)
	for i := 0; i < 101; i++ {
		end_record[i] = byte(0)
	}
	conn.Write(end_record)
	ch <- true

}

func connectToServers(ch chan<- bool, port string, serverId int, scs ServerConfigs, serverMap map[int][][]byte) {
	for _, server := range scs.Servers {
		send_data := serverMap[server.ServerId]
		if server.ServerId == serverId {
			continue
		} else {
			for {
				conn, err := net.Dial("tcp", server.Host+":"+server.Port)
				if err == nil {
					go sendData(conn, send_data, ch)
					break
				} else {
					continue
				}
			}
		}
	}

}
func checkAllRecordSent(main_ch chan<- bool, read_only_ch <-chan bool, numOfClients int) {
	count := 0
	for {
		bool_val := <-read_only_ch
		if bool_val {
			count += 1
		}
		if count == numOfClients {
			break
		}
	}
	main_ch <- true

}

func collectRecord(read_only_ch <-chan []byte, numOfClients int) [][]byte {
	count := 0
	var records [][]byte
	for {
		record := <-read_only_ch
		if record[0] == byte(0) {
			count += 1

		} else {
			records = append(records, record[1:])
		}
		if count == numOfClients {
			break
		}
	}
	return records
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	// Implement Distributed Sort

	var host, port string
	for _, server := range scs.Servers {
		if server.ServerId == serverId {
			host = server.Host
			port = server.Port
		}
	}

	serverMap := partitionServerFromInput(os.Args[2], scs)
	records := serverMap[serverId]
	main_ch := make(chan bool)

	if len(scs.Servers) > 1 {

		listen_ch := make(chan []byte) // bidirection channel
		// putting data in channel
		go listenForClientConnections(listen_ch, host, port)

		send_ch := make(chan bool)
		// sending data to servers
		go connectToServers(send_ch, port, serverId, scs, serverMap)

		numOfClients := len(scs.Servers) - 1
		go checkAllRecordSent(main_ch, send_ch, numOfClients)
		infile_records := collectRecord(listen_ch, numOfClients)
		records = append(infile_records, records...) // concatination
	}

	// sort records
	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i][0:10], records[j][0:10]) < 0 })

	// create output file
	f, err := os.Create(os.Args[3])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var chunkRecords []byte // assemble the slice of byte array to a chunck of byte array
	for _, record := range records {
		chunkRecords = append(chunkRecords, record...)
	}
	_, write_err := f.Write(chunkRecords)
	if write_err != nil {
		log.Fatal(err)
	}
	if len(scs.Servers) > 1 {
		for {
			all_done := <-main_ch
			if all_done {
				break
			}
		}
	}
}
