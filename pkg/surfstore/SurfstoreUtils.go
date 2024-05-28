package surfstore

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func CompareBlockHashList(h1, h2 []string) bool {
	if len(h1) != len(h2) {
		return false
	}
	for i := range h1 {
		if h1[i] != h2[i] {
			return false
		}
	}
	return true
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	baseDir, localFileInfoMap, err := getLocalInfo(client)
	if localFileInfoMap == nil {
		localFileInfoMap = make(map[string]*FileMetaData)
	}
	err = updateLocalIndexFile(client, err, baseDir, localFileInfoMap)
	//err = debugUpdateLocalFile(localFileInfoMap, err, baseDir)
	fmt.Println("finish update local index")

	remoteIndex, err := getRemoteIndexFile(client, err)
	if err != nil {
		log.Fatalf("Error while getting remote index file: %v", err)
	}
	if remoteIndex == nil {
		remoteIndex = make(map[string]*FileMetaData)
	}
	fmt.Println("finish get remote index")
	// debug
	// remoteIndex, err := LoadMetaFromMetaFile("testremote")
	// fmt.Println("start print remote index")
	// for k, v := range remoteIndex {
	//	 fmt.Println(k, v.BlockHashList[0], v.Version)
	// }
	// fmt.Println("finish print remote index")
	// debug finish

	// ************************************************************
	// First, it is possible that the remote index refers to a file not present in the local index or in the
	// base directory. In this case, the client should download the blocks associated with that file,
	// reconstitute that file in the base directory, and then add the updated FileInfo information to the
	// local index.
	// ************************************************************

	// download file logic
	// - get block store address
	// - get block from server
	// - write block to file
	// - update local index

	// upload file logic (need handle conflict)
	// - get block store address
	// - put block to block store (block must be put before update index)
	// - update remote index

	// delete local file logic
	// - delete local file
	// - update local index
	//  - version++
	//  - block hash list = "0"

	// delete remote file logic
	// - update remote index
	//  - version++
	//  - block hash list = "0"

	// logic
	// - local index no file -> download file (done)
	// - local index has file (done)
	//   - remote index no file -> upload file (done)
	//   - remote index has file -> compare version (done)
	//     - local version > remote version -> check local hash[0] (done)
	//       - local hash[0] == "0" -> delete remote file (done)
	//       - local hash[0] != "0" -> upload file (done)
	//     - local version < remote version -> check remote hash[0] (done)
	//       - remote hash[0] == "0" -> delete local file (done)
	//       - remote hash[0] != "0" -> download file (done)
	//     - local version = remote version -> compare block hash list (done)
	//       - local hash list == remote hash list -> sync with remote, do nothing (done)
	//       - local hash list != remote hash list -> there is conflict, tell client, and download/delete local file (done)
	// 		   - same as local version < remote version (done)

	// TODO: I guess the conflict handle is wrong, for in the instruction,
	// version confilct should be checked by uploadblock -> success.
	// but I think this version still works fine.

	// local index no file -> download file
	for remoteFilename, remoteFileMetaData := range remoteIndex {
		//fmt.Println("remote file name: ", remoteFilename)
		//fmt.Println("remote file version: ", remoteFileMetaData.Version)
		if _, ok := localFileInfoMap[remoteFilename]; !ok {
			if remoteFileMetaData.BlockHashList[0] != "0" { // remote file is not deleted, download file
				downloadFile(client, remoteFileMetaData, err, remoteFilename, localFileInfoMap)
			} else {
				// remote file is deleted, update local index
				localFileInfoMap[remoteFilename] = remoteFileMetaData
				//fmt.Println("delete local file: ", remoteFilename)
			}
		} else {
			//fmt.Println("local file version: ", localFileInfoMap[remoteFilename].Version)
			// local index has file, remote index has file -> compare version
			localFileMetaData := localFileInfoMap[remoteFilename]
			if localFileMetaData.Version > remoteFileMetaData.Version {
				// - local hash[0] == "0" -> delete remote file
				// - local hash[0] != "0" -> upload file
				if localFileMetaData.BlockHashList[0] == "0" {
					// delete remote file
					remoteFileMetaData.Version = localFileMetaData.Version
					remoteFileMetaData.BlockHashList = []string{"0"}
					remoteIndex[remoteFilename] = remoteFileMetaData
					//fmt.Println("Delete remote file: ", remoteFilename)
					//fmt.Println("version: ", remoteFileMetaData.Version)

					var returnedVersion int32
					err = client.UpdateFile(remoteFileMetaData, &returnedVersion)
					if err != nil {
						log.Fatalf("Error while updating file %s to the server: %v", remoteFilename, err)
					}
					if returnedVersion == -1 {
						// conflict, failed to update
						//fmt.Println("Conflict: %s, unsuccessful remote change, download/delete local file", remoteFilename)
						// get new remote index
						remoteIndex, err = getRemoteIndexFile(client, err)
						if err != nil {
							log.Fatalf("Error while getting remote index file: %v", err)
						}
						if remoteIndex == nil {
							remoteIndex = make(map[string]*FileMetaData)
						}
						// download file/ delete local file
						remoteFileMetaData = remoteIndex[remoteFilename]
						if remoteFileMetaData.BlockHashList[0] == "0" {
							// delete local file
							err = os.Remove(filepath.Join(baseDir, remoteFilename))
							if err != nil {
								log.Fatalf("Error while deleting file %s: %v", remoteFilename, err)
							}
							// update local index
							localFileInfoMap[remoteFilename] = remoteFileMetaData
							//fmt.Println("Delete local file: ", remoteFilename)
						} else {
							downloadFile(client, remoteFileMetaData, err, remoteFilename, localFileInfoMap)
						}
					}
				} else {
					// upload file
					//Each time when you update a file, your program will break the file
					//into blocks and compute hash values for each block (you’ve already
					//implemented this in P3). Then instead of calling GetBlockStoreAddr,
					//this time we will call GetBlockStoreMap which returns a map indicating
					//which servers the blocks belong to based on the consistent hashing
					//algorithm covered in the lecture. Based on this map, you can upload
					//your blocks to corresponding block servers.

					// ToDo: (method 1) -> not good...
					// a file -> we have a list of block hashes in localFileMetaData.BlockHashList
					// GetBlockStoreMap -> blockStoreMap: server address -> block hashes
					// for each server address in blockStoreMap:
					//     upload block to server address
					// update remote index...

					// hard to find block related to block hash

					// ToDo: (method 2) -> better
					// a file -> we have a list of block hashes in localFileMetaData.BlockHashList
					// GetBlockStoreMap -> blockStoreMap: server address -> block hashes
					// for each block hash in localFileMetaData.BlockHashList:
					//     find the server address in blockStoreMap
					//     upload block to server address

					// ToDo: (method 3) -> use this
					// a file -> we have a list of block hashes in localFileMetaData.BlockHashList
					// GetBlockStoreMap -> blockStoreMap: server address -> block hashes
					// change map to block hash -> server address
					// for each block hash in localFileMetaData.BlockHashList:
					//     find the server address in blockStoreMap
					//     upload block to server address

					// localFileMetaData.BlockHashList -> blockstoremap
					blockStoreMap := map[string][]string{}
					err = client.GetBlockStoreMap(localFileMetaData.BlockHashList, &blockStoreMap)
					if err != nil {
						log.Fatalf("Error while getting block store map from the server: %v", err)
					}
					// change map to block hash -> server address
					hashToServer := map[string]string{}
					for serverAddr, blockHashes := range blockStoreMap {
						for _, blockHash := range blockHashes {
							hashToServer[blockHash] = serverAddr
						}
					}

					localPath := filepath.Join(client.BaseDir, remoteFilename)
					file, err := os.Open(localPath)
					if err != nil {
						log.Fatalf("Cannot open file %s: %v", localPath, err)
					}
					defer file.Close()

					// get block num for the file
					filestats, err := file.Stat()
					if err != nil {
						log.Fatalf("Cannot get file stats %s: %v", localPath, err)
					}
					blocknum := filestats.Size() / int64(client.BlockSize)
					if filestats.Size()%int64(client.BlockSize) != 0 {
						blocknum++
					}
					//fmt.Println("upload block num: ", blocknum)

					// upload block to block store
					if blocknum == 0 {
						// TODO: no need to upload block if blocknum == 0, need change logic
						//var block Block
						//block.BlockData = nil
						//block.BlockSize = 0
						//var success bool
						////err = client.PutBlock(&block, blockStoreAddr, &success)
						//if err != nil || !success {
						//	log.Fatalf("Error while putting block %d to the server: %v", 0, err)
						//}
						// no need to upload block
					} else {
						blockData := make([]byte, client.BlockSize)
						for i := int64(0); i < blocknum; i++ {
							n, err := file.Read(blockData)
							if err != nil {
								log.Fatalf("Cannot read block %d from file %s: %v", i, localPath, err)
							}
							var block Block
							block.BlockData = blockData[:n]
							block.BlockSize = int32(n)

							// get block store address
							blockStoreAddr := hashToServer[GetBlockHashString(block.BlockData)]
							var success bool
							err = client.PutBlock(&block, blockStoreAddr, &success)
							if err != nil || !success {
								log.Fatalf("Error while putting block %d to the server: %v", i, err)
							}
						}
					}

					// update remote index
					remoteFileMetaData.Version = localFileMetaData.Version
					remoteFileMetaData.BlockHashList = localFileMetaData.BlockHashList
					remoteIndex[remoteFilename] = remoteFileMetaData
					//fmt.Println("Upload file: ", remoteFilename)

					var returnedVersion int32
					err = client.UpdateFile(remoteFileMetaData, &returnedVersion)
					if err != nil {
						log.Fatalf("Error while updating file %s to the server: %v", remoteFilename, err)
					}
					if returnedVersion == -1 {
						// conflict, failed to update
						//fmt.Println("Conflict: %s, unsuccessful remote change, download/delete local file", remoteFilename)
						// get new remote index
						remoteIndex, err = getRemoteIndexFile(client, err)
						if err != nil {
							log.Fatalf("Error while getting remote index file: %v", err)
						}
						if remoteIndex == nil {
							remoteIndex = make(map[string]*FileMetaData)
						}
						// download file/ delete local file
						remoteFileMetaData = remoteIndex[remoteFilename]
						if remoteFileMetaData.BlockHashList[0] == "0" {
							// delete local file
							err = os.Remove(filepath.Join(baseDir, remoteFilename))
							if err != nil {
								log.Fatalf("Error while deleting file %s: %v", remoteFilename, err)
							}
							// update local index
							localFileInfoMap[remoteFilename] = remoteFileMetaData
							//fmt.Println("Delete local file: ", remoteFilename)
						} else {
							downloadFile(client, remoteFileMetaData, err, remoteFilename, localFileInfoMap)
						}
					}
				}
			} else if localFileMetaData.Version < remoteFileMetaData.Version {
				//fmt.Println("a local old file: ", remoteFilename)
				// - remote hash[0] == "0" -> delete local file
				// - remote hash[0] != "0" -> download file
				if remoteFileMetaData.BlockHashList[0] == "0" {
					// delete local file
					err = os.Remove(filepath.Join(baseDir, remoteFilename))
					if err != nil {
						log.Fatalf("Error while deleting file %s: %v", remoteFilename, err)
					}
					// update local index
					localFileInfoMap[remoteFilename] = remoteFileMetaData
					//fmt.Println("Delete local file: ", remoteFilename)
				} else {
					downloadFile(client, remoteFileMetaData, err, remoteFilename, localFileInfoMap)
				}
			} else if localFileMetaData.Version == remoteFileMetaData.Version {
				// - local hash list == remote hash list -> sync with remote, do nothing
				// - local hash list != remote hash list -> there is conflict, tell client, and download/delete local file
				// - same as local version < remote version
				if !CompareBlockHashList(localFileMetaData.BlockHashList, remoteFileMetaData.BlockHashList) {
					//fmt.Println("Conflict: %s, unsuccessful local change, sync with remote", remoteFilename)
					if remoteFileMetaData.BlockHashList[0] == "0" {
						// delete local file
						err = os.Remove(filepath.Join(baseDir, remoteFilename))
						if err != nil {
							log.Fatalf("Error while deleting file %s: %v", remoteFilename, err)
						}
						// update local index
						localFileInfoMap[remoteFilename] = remoteFileMetaData
						//fmt.Println("Delete local file: ", remoteFilename)
					} else {
						downloadFile(client, remoteFileMetaData, err, remoteFilename, localFileInfoMap)
					}
				} else {
					// sync with remote, do nothing
				}
			}
		}
	}

	// - local index has file
	//   - remote index no file -> upload file (done)
	for localFilename, localFileMetaData := range localFileInfoMap {
		if _, ok := remoteIndex[localFilename]; !ok {
			if localFileMetaData.BlockHashList[0] != "0" {
				// upload file
				// localFileMetaData.BlockHashList -> blockstoremap
				blockStoreMap := map[string][]string{}
				err = client.GetBlockStoreMap(localFileMetaData.BlockHashList, &blockStoreMap)
				if err != nil {
					log.Fatalf("Error while getting block store map from the server: %v", err)
				}
				// change map to block hash -> server address
				hashToServer := map[string]string{}
				for serverAddr, blockHashes := range blockStoreMap {
					for _, blockHash := range blockHashes {
						hashToServer[blockHash] = serverAddr
					}
				}

				localPath := filepath.Join(client.BaseDir, localFilename)
				file, err := os.Open(localPath)
				if err != nil {
					log.Fatalf("Cannot open file %s: %v", localPath, err)
				}
				defer file.Close()

				//blockStoreAddr := ""
				//client.GetBlockStoreAddr(&blockStoreAddr)

				// get block num for the file
				filestats, err := file.Stat()
				if err != nil {
					log.Fatalf("Cannot get file stats %s: %v", localPath, err)
				}
				blocknum := filestats.Size() / int64(client.BlockSize)
				if filestats.Size()%int64(client.BlockSize) != 0 {
					blocknum++
				}

				// upload block to block store
				if blocknum == 0 {
					//var block Block
					//block.BlockData = nil
					//block.BlockSize = 0
					//var success bool
					//
					////err = client.PutBlock(&block, blockStoreAddr, &success)
					//if err != nil || !success {
					//	log.Fatalf("Error while putting block %d to the server: %v", 0, err)
					//}
				} else {
					blockData := make([]byte, client.BlockSize)
					for i := int64(0); i < blocknum; i++ {
						n, err := file.Read(blockData)
						if err != nil {
							log.Fatalf("Cannot read block %d from file %s: %v", i, localPath, err)
						}
						var block Block
						block.BlockData = blockData[:n]
						block.BlockSize = int32(n)
						var success bool
						blockStoreAddr := hashToServer[GetBlockHashString(block.BlockData)]
						err = client.PutBlock(&block, blockStoreAddr, &success)
						if err != nil || !success {
							log.Fatalf("Error while putting block %d to the server: %v", i, err)
						}
					}
				}

				// update remote index
				remoteFileMetaData := &FileMetaData{
					Filename:      localFilename,
					Version:       localFileMetaData.Version,
					BlockHashList: localFileMetaData.BlockHashList,
				}
				remoteIndex[localFilename] = remoteFileMetaData
				//fmt.Println("Upload file: ", localFilename)
				remoteFilename := localFilename

				var returnedVersion int32
				err = client.UpdateFile(remoteFileMetaData, &returnedVersion)
				if err != nil {
					log.Fatalf("Error while updating file %s to the server: %v", remoteFilename, err)
				}
				if returnedVersion == -1 {
					// conflict, failed to update
					//fmt.Println("Conflict: %s, unsuccessful remote change, download/delete local file", remoteFilename)
					// get new remote index
					remoteIndex, err = getRemoteIndexFile(client, err)
					if err != nil {
						log.Fatalf("Error while getting remote index file: %v", err)
					}
					if remoteIndex == nil {
						remoteIndex = make(map[string]*FileMetaData)
					}
					// download file/ delete local file
					remoteFileMetaData = remoteIndex[remoteFilename]
					if remoteFileMetaData.BlockHashList[0] == "0" {
						// delete local file
						err = os.Remove(filepath.Join(baseDir, remoteFilename))
						if err != nil {
							log.Fatalf("Error while deleting file %s: %v", remoteFilename, err)
						}
						// update local index
						localFileInfoMap[remoteFilename] = remoteFileMetaData
						//fmt.Println("Delete local file: ", remoteFilename)
					} else {
						downloadFile(client, remoteFileMetaData, err, remoteFilename, localFileInfoMap)
					}
				}
			}
		}
	}

	// finish sync, save remote index to index.db, save local index to index.
	// debug
	// fmt.Println(remoteIndex["file7.jpg"].Version)
	// err = WriteMetaFile(remoteIndex, baseDir)
	//if err != nil {
	//	log.Fatalf("Error while writing metadata to index.db: %v", err)
	//}
	//fmt.Println("write to index.db")
	err = WriteMetaFile(localFileInfoMap, baseDir)
	//fmt.Println("finish sync")

	// upload remote index to server

	// debug
	//fmt.Println("start print local index")
	//for k, v := range localFileInfoMap {
	//	fmt.Println(k, v.BlockHashList[0], v.Version)
	//}
	//fmt.Println("finish print local index")
}

func downloadFile(client RPCClient, remoteFileMetaData *FileMetaData, err error, remoteFilename string, localFileInfoMap map[string]*FileMetaData) {
	// download file
	//fmt.Println("downloading file")
	//blockStoreAddr := ""
	//client.GetBlockStoreAddr(&blockStoreAddr)

	//blockStoreAddrs := []string{}
	//err = client.GetBlockStoreAddrs(&blockStoreAddrs)
	//if err != nil {
	//	log.Fatalf("Error while getting block store address from the server: %v", err)
	//}
	// check remoteFileMetaData, if len = 1 and hash = -1, empty file, no need to download
	// only open local path and exit
	if len(remoteFileMetaData.BlockHashList) == 1 && remoteFileMetaData.BlockHashList[0] == "-1" {
		localPath := ConcatPath(client.BaseDir, remoteFilename)
		localFile, err := os.Create(localPath)
		if err != nil {
			log.Fatalf("Cannot create file %s: %v", localPath, err)
		}
		defer localFile.Close()
		localFileInfoMap[remoteFilename] = remoteFileMetaData
		return
	}

	blockStoreMap := map[string][]string{}
	err = client.GetBlockStoreMap(remoteFileMetaData.BlockHashList, &blockStoreMap)
	if err != nil {
		log.Fatalf("Error while getting block store map from the server: %v", err)
	}
	// change map to block hash -> server address
	hashToServer := map[string]string{}
	for serverAddr, blockHashes := range blockStoreMap {
		for _, blockHash := range blockHashes {
			hashToServer[blockHash] = serverAddr
		}
	}

	localPath := ConcatPath(client.BaseDir, remoteFilename)
	localFile, err := os.Create(localPath)
	if err != nil {
		log.Fatalf("Cannot create file %s: %v", localPath, err)
	}
	defer localFile.Close()

	for _, blockHash := range remoteFileMetaData.BlockHashList {
		var block Block
		// get block store address
		blockStoreAddr := hashToServer[blockHash]
		err = client.GetBlock(blockHash, blockStoreAddr, &block)
		if err != nil {
			log.Fatalf("Error while getting block %s from the server: %v", blockHash, err)
		}

		// sync write block to file
		_, err = localFile.Write(block.BlockData)
		if err != nil {
			log.Fatalf("Error while writing block to file %s: %v", localPath, err)
		}
	}

	// update local index
	localFileInfoMap[remoteFilename] = remoteFileMetaData
	//fmt.Println("Download file: ", remoteFilename)
}

func getRemoteIndexFile(client RPCClient, err error) (map[string]*FileMetaData, error) {
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Fatalf("Error while getting FileInfoMap from the server: %v", err)
	}
	return remoteIndex, err
}

func debugUpdateLocalFile(localFileInfoMap map[string]*FileMetaData, err error, baseDir string) error {
	// debug
	fmt.Println("start print local index")
	for k, v := range localFileInfoMap {
		fmt.Println(k, v.BlockHashList[0], v.Version)
	}
	fmt.Println("finish print local index")
	// temperay write into index.db
	err = WriteMetaFile(localFileInfoMap, baseDir)
	if err != nil {
		log.Fatalf("Error while writing metadata to index.db: %v", err)
	} // seems work fine

	// debug finish
	return err
}

func updateLocalIndexFile(client RPCClient, err error, baseDir string, localFileInfoMap map[string]*FileMetaData) error {
	err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && info.Name() != ".DS_Store" && info.Name() != "index.db" {
			//fmt.Println("file name: ", info.Name())
			//fmt.Println("convert to hash")
			file, err := os.Open(path)
			if err != nil {
				log.Printf("Cannot open file %s: %v", path, err)
				return nil
			}
			defer file.Close()

			blocknum := info.Size() / int64(client.BlockSize)
			if info.Size()%int64(client.BlockSize) != 0 {
				blocknum++
			}
			// Todo: remember to handle blocknum = 0 (done)
			//fmt.Println(blocknum)

			// block -> hash list (single file)
			blockHashList := make([]string, blocknum)
			if blocknum > 0 {
				err2, done := blockToHash(path, blocknum, client, file, blockHashList)
				if done {
					return err2
				}
			} else {
				//In index.db, an empty file has a row that a single hash value of “-1”
				//(a string with two characters). It’s like the following in the table.
				blockHashList = make([]string, 1)
				//blockHashList[0] = GetBlockHashString(nil)
				blockHashList[0] = "-1"
			}

			// debug
			//fmt.Println("block num: ", blocknum)
			//fmt.Println("len block hash list: ", len(blockHashList))
			//fmt.Println("block hash list: ", blockHashList[0])
			// debug finish

			compareLocalIndexFile(localFileInfoMap, info, blockHashList, baseDir) // compare with local index file
		}
		return nil
	})
	checkLocalDelete(localFileInfoMap, baseDir) // mark deleted files
	return err
}

func getLocalInfo(client RPCClient) (string, map[string]*FileMetaData, error) {
	baseDir := client.BaseDir
	localFileInfoMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Fatalf("Error while loading metadata from index.db: %v", err)
	}
	return baseDir, localFileInfoMap, err
}

func checkLocalDelete(localFileInfoMap map[string]*FileMetaData, baseDir string) {
	for filename, localFileMetaData := range localFileInfoMap {
		filePath := filepath.Join(baseDir, filename)
		_, err := os.Stat(filePath)
		//fmt.Println("file path: ", filePath)
		if err != nil {
			//fmt.Println("file not exist: ", filename)
			if localFileMetaData.BlockHashList[0] != "0" {
				// file not exist -> mark as deleted
				localFileInfoMap[filename] = &FileMetaData{
					Filename:      filename,
					Version:       localFileMetaData.Version + 1,
					BlockHashList: []string{"0"},
				}
				//fmt.Println("File deleted: ", filename)
			}
		}
	}
}

func compareLocalIndexFile(localFileInfoMap map[string]*FileMetaData, info os.FileInfo, blockHashList []string, baseDir string) {
	if localFileMetaData, ok := localFileInfoMap[info.Name()]; ok {

		// debug
		//fmt.Println("local hash: ", localFileMetaData.BlockHashList[0])
		//fmt.Println("remote hash: ", blockHashList[0])
		// debug finish

		if !CompareBlockHashList(localFileMetaData.BlockHashList, blockHashList) {
			// file has changed -> update local index file
			localFileInfoMap[info.Name()] = &FileMetaData{
				Filename:      info.Name(),
				Version:       localFileMetaData.Version + 1,
				BlockHashList: blockHashList,
			}
			//fmt.Println("File has changed: ", info.Name())
			//fmt.Println("Version: ", localFileMetaData.Version+1)
		}
	} else {
		// new file -> update local index file
		localFileInfoMap[info.Name()] = &FileMetaData{
			Filename:      info.Name(),
			Version:       1, //try version 1...
			BlockHashList: blockHashList,
		}
		//fmt.Println("New file: ", info.Name())
		//fmt.Println("Version: ", 1)
	}
}

func blockToHash(path string, blocknum int64, client RPCClient, file *os.File, blockHashList []string) (error, bool) {
	for i := int64(0); i < blocknum; i++ {
		block := make([]byte, client.BlockSize)
		n, err := file.Read(block)
		if err != nil {
			log.Printf("Cannot read block %d from file %s: %v", i, path, err)
			return nil, true
		}
		blockHashList[i] = GetBlockHashString(block[:n])

		// debug
		//if i == 0 {
		//	fmt.Println("block hash: ", blockHashList[i])
		//}
		// debug finish
	}
	return nil, false
}
