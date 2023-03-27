package surfstore

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	reflect "reflect"
	"strings"
)

// Implement the logic for a client syncing with the server here.

/*
RPCClient
MetaStoreAddr: hostPort,
BaseDir:       baseDir,
BlockSize:     blockSize,
*/

func updateLocalFile(localFileMetaData map[string]*FileMetaData, baseDir string, blockSize int) (e error) {
	// check update file, new file, delete file
	files, readDirErr := ioutil.ReadDir(baseDir)
	if readDirErr != nil {
		log.Println("Error Reading Directory", readDirErr)
	}

	currFileMap := make(map[string][]string)
	for _, file := range files {
		if file.Name() != DEFAULT_META_FILENAME {
			fileStat, osErr := os.Stat(baseDir + "/" + file.Name())
			if osErr == nil && fileStat.IsDir() {
				log.Println("The Base Directory Should be Flat")
				continue
			}

			if strings.Contains(file.Name(), "/") || strings.Contains(file.Name(), ",") {
				log.Println("Invalid File Name")
				continue
			}
			var numBlocks int = int(math.Ceil(float64(file.Size()) / float64(blockSize)))
			fileDiscriptor, fileReadErr := os.Open(baseDir + "/" + file.Name())
			if fileReadErr != nil {
				log.Println("Error Reading File In Basedir: ", fileReadErr)
				return fileReadErr
			}

			for i := 0; i < numBlocks; i++ {
				buffer := make([]byte, blockSize)
				len, readErr := fileDiscriptor.Read(buffer)
				if readErr != nil {
					log.Println("Error Reading Bytes from File: ", readErr)
					return readErr
				}
				buffer = buffer[:len]
				fileHash := GetBlockHashString(buffer)
				currFileMap[file.Name()] = append(currFileMap[file.Name()], fileHash)
			}

			fileMetaData, ok := localFileMetaData[file.Name()]
			if ok {
				// deep equal checks for slice equality, in this case, check two hash block lists update
				if !reflect.DeepEqual(currFileMap[file.Name()], fileMetaData.BlockHashList) {
					localFileMetaData[file.Name()].BlockHashList = currFileMap[file.Name()]
					localFileMetaData[file.Name()].Version++
				}
			} else {
				// new file
				metadata := FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: currFileMap[file.Name()]}
				localFileMetaData[file.Name()] = &metadata
			}

		}
	}
	// check delete file
	for fileName, metaData := range localFileMetaData {
		_, ok := currFileMap[fileName]
		if !ok {

			if len(metaData.BlockHashList) != 1 || metaData.BlockHashList[0] != "0" { // file already deleted
				metaData.Version++
				metaData.BlockHashList = []string{"0"}
			}

		}
	}
	return nil

}
func uploadToServer(fileMetaData *FileMetaData, client *RPCClient, blockStoreAddr string) (e error) {
	baseDir, _ := filepath.Abs(client.BaseDir)
	path := ConcatPath(baseDir, fileMetaData.Filename)
	var latestVersion int32

	// upload a deleted file
	fileStat, osErr := os.Stat(path)
	if errors.Is(osErr, os.ErrNotExist) {
		updateErr := client.UpdateFile(fileMetaData, &latestVersion)
		if updateErr != nil {
			log.Println("Error Updating Deleted File ", updateErr)
		}
		fileMetaData.Version = latestVersion
		return updateErr
	} else if osErr == nil {
		file, osErr := os.Open(path)
		if osErr != nil {
			log.Println("Error Opening File: ", osErr)
			return osErr
		}
		defer file.Close()

		numBlocks := int(math.Ceil(float64(fileStat.Size()) / float64(client.BlockSize)))
		for i := 0; i < numBlocks; {
			bytes := make([]byte, client.BlockSize)
			len, readErr := file.Read(bytes)
			if readErr != nil && readErr != io.EOF {
				log.Println("Error Reading Bytes From File: ", readErr)
			}
			bytes = bytes[:len]
			block := Block{BlockData: bytes, BlockSize: int32(len)}

			var success bool
			blockUploadErr := client.PutBlock(&block, blockStoreAddr, &success)
			if !success {
				log.Println("Error Putting Block: ", blockUploadErr)
			}
			i++
		}

		updateErr := client.UpdateFile(fileMetaData, &latestVersion)
		if updateErr != nil {
			log.Println("Error Updating File: ", updateErr)
		}
		fileMetaData.Version = latestVersion
		return updateErr

	}

	return osErr
}

func updateServerWithLocal(updatedLocalMeta map[string]*FileMetaData, remoteMeta map[string]*FileMetaData, client *RPCClient, blockStoreAddr string) {
	// server update(local has new file version > server version)
	for fileName, metaData := range updatedLocalMeta {
		if remoteMetaData, ok := remoteMeta[fileName]; ok {
			if metaData.Version > remoteMetaData.Version {
				//if there are some files in local index that have been updated
				uploadErr := uploadToServer(metaData, client, blockStoreAddr)
				if uploadErr != nil {
					log.Println("Error Uploading "+fileName+" :", uploadErr)
				}
			}
		} else {
			//if there are some files that are newly added
			uploadErr := uploadToServer(metaData, client, blockStoreAddr)
			if uploadErr != nil {
				log.Println("Error Uploading "+fileName+" :", uploadErr)
			}
		}
	}
}
func downloadFromServer(remoteFileMetaData *FileMetaData, client *RPCClient, blockStoreAddr string) (e error) {
	baseDir, _ := filepath.Abs(client.BaseDir)
	path := ConcatPath(baseDir, remoteFileMetaData.Filename)
	file, osErr := os.Create(path)
	if osErr != nil {
		log.Println("Error Creating/Trancating File: ", osErr)
	}
	defer file.Close()

	if len(remoteFileMetaData.BlockHashList) == 1 && remoteFileMetaData.BlockHashList[0] == "0" {
		deleteErr := os.Remove(path)
		if deleteErr != nil {
			log.Println("Error Deleting File: ", deleteErr)
			return deleteErr
		}
		return nil
	}

	content := ""
	var hashs []string
	_ = client.HasBlocks(remoteFileMetaData.BlockHashList, blockStoreAddr, &hashs)
	for _, hash := range hashs {

		var block Block
		blockErr := client.GetBlock(hash, blockStoreAddr, &block)
		if blockErr != nil {
			log.Println("Error Getting Remote Block: ", blockErr)
		}
		content += string(block.BlockData)
	}
	file.WriteString(content)

	return nil

}

func updateLocalWithServer(updatedLocalMeta map[string]*FileMetaData, remoteMeta map[string]*FileMetaData, client *RPCClient, blockStoreAddr string) {
	// local update(local has new file version < server version)
	for fileName, metaData := range remoteMeta {
		localMetaData, ok := updatedLocalMeta[fileName]
		if ok {
			if localMetaData.Version < metaData.Version {
				*localMetaData = *metaData
				_ = downloadFromServer(metaData, client, blockStoreAddr)
			} else if localMetaData.Version == metaData.Version && !reflect.DeepEqual(localMetaData.BlockHashList, metaData.BlockHashList) {
				*localMetaData = *metaData
				_ = downloadFromServer(metaData, client, blockStoreAddr)
			}
		} else {
			updatedLocalMeta[fileName] = &FileMetaData{}
			*updatedLocalMeta[fileName] = *metaData
			downloadFromServer(metaData, client, blockStoreAddr)
		}
	}
}

func ClientSync(client RPCClient) {

	baseDir, _ := filepath.Abs(client.BaseDir)

	// Check if the client directory exist
	if _, fileErr := os.Stat(baseDir); errors.Is(fileErr, os.ErrNotExist) {
		log.Println("Basedir Not Exist! Creating One", fileErr)
		err := os.Mkdir(baseDir, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}

	localFileMetaData, localSyncErr := LoadMetaFromMetaFile(baseDir)
	if localSyncErr != nil {
		log.Println("Error Syncing With Local Database Record:", localSyncErr)
	}

	blockSize := client.BlockSize

	// sync local file, check deleted file and write file

	localUpdateErr := updateLocalFile(localFileMetaData, baseDir, blockSize)
	if localUpdateErr != nil {
		log.Println(localUpdateErr)
	}

	// sync with server,upload or download

	var blockStoreAddr string
	blockAddrErr := client.GetBlockStoreAddr(&blockStoreAddr)
	if blockAddrErr != nil {
		log.Println("Error Getting BlockStoreAddr: ", blockAddrErr)
	}

	var remoteMeta map[string]*FileMetaData
	remoteGetErr := client.GetFileInfoMap(&remoteMeta)
	if remoteGetErr != nil {
		log.Println("Error Getting Metadata From Server: ", remoteGetErr)
	}

	updateServerWithLocal(localFileMetaData, remoteMeta, &client, blockStoreAddr) // upload

	updateLocalWithServer(localFileMetaData, remoteMeta, &client, blockStoreAddr) // download

	// write to db
	writeError := WriteMetaFile(localFileMetaData, client.BaseDir)
	if writeError != nil {
		log.Println("Error Writing Back to Database", writeError)
	}

}
