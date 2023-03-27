package main

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"sort"
)

func readAndMap(filename string, chunkSize int) map[string][]byte {
	f, err := os.Open(filename) // Open is read only
	if err != nil {
		log.Fatal(err)
	}

	dic := make(map[string][]byte)
	reader := bufio.NewReader(f)
	for {
		buf := make([]byte, chunkSize)
		n, err := io.ReadFull(reader, buf)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		if err == io.EOF {
			break
		}

		key := string(buf[:10])
		kv := make([]byte, 100)

		copy(kv, buf[:n])
		dic[key] = kv

	}
	f.Close()
	return dic
}

func writeOnSortedKeys(filename string, dic map[string][]byte, keys [][]byte) {
	f, err := os.Create(filename)
	writer := bufio.NewWriter(f)

	for _, k := range keys {
		_, err = writer.Write(dic[string(k)])
		if err != nil {
			log.Fatal(err)
		}
	}
	writer.Flush()
	f.Close()

}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v inputfile outputfile\n", os.Args[0])
	}
	log.Printf("Sorting %s to %s\n", os.Args[1], os.Args[2])
	dic := readAndMap(os.Args[1], 100)
	keys := make([][]byte, len(dic))
	i := 0
	for k := range dic {
		keys[i] = []byte(k)
		i++
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	writeOnSortedKeys(os.Args[2], dic, keys)
}
