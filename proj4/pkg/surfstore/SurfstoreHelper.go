package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes(fileName,version,hashIndex,hashValue) values(?,?,?,?)`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)

	if err != nil {
		log.Fatal("Error During Meta Write Back: ", err)
		return err
	}
	defer statement.Close()
	statement.Exec()

	insertStatement, err := db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During Initializing Statement: ", err)
		return err
	}

	defer insertStatement.Close()
	if err != nil {
		log.Fatal("Error During Initializing Statement: ", err)
		return err
	}
	for _, metaData := range fileMetas {
		for i, hl := range metaData.BlockHashList {
			_, err = insertStatement.Exec(metaData.Filename, metaData.Version, i, hl)
			if err != nil {
				log.Fatal("Error Writing Date Into Database: ", err)
				return err
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/

/*

const getDistinctFileName string = `Select distinct fileName from indexes`

func getTuplesByFileName(filename string) string {
	query := fmt.Sprintf(`Select fileName, hashValue from indexes order by hashIndex where fileName=%s`, filename)
	return query
}
*/

const getAllRows string = `select * from indexes`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, fileError := os.Stat(metaFilePath)

	// why check isdir here
	if fileError != nil || metaFileStats.IsDir() {
		// No db file exist, create a new one
		if errors.Is(fileError, os.ErrNotExist) {
			_, fileCreateErr := os.Create(metaFilePath)
			if fileCreateErr != nil {
				log.Fatal("Error When Create db File")
			}

		}
		return fileMetaMap, nil
	}

	db, dbOpenError := sql.Open("sqlite3", metaFilePath)
	if dbOpenError != nil {
		log.Fatal("Error When Opening Meta")
	}
	rows, QueryErr := db.Query(getAllRows)
	if QueryErr != nil {
		log.Fatal("Error During Querying Database ", QueryErr)
		return fileMetaMap, QueryErr
	}
	defer rows.Close()
	for rows.Next() {

		var fileName string
		var version int32
		var hashIndex int
		var hashValue string

		scanError := rows.Scan(&fileName, &version, &hashIndex, &hashValue)

		if scanError != nil {
			log.Fatal("Error During Scanning Database Row:", scanError)
			return fileMetaMap, scanError
		}
		_, ok := fileMetaMap[fileName]
		if ok {
			//TODO Distinct file name check
			fileMetaMap[fileName].BlockHashList = append(fileMetaMap[fileName].BlockHashList, hashValue)
		} else {
			var data *FileMetaData = new(FileMetaData)
			data.Filename = fileName
			data.Version = version
			data.BlockHashList = append(data.BlockHashList, hashValue)
			fileMetaMap[fileName] = data
		}

	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
