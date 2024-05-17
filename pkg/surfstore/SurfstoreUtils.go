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
	//The client should first scan the base directory, and for each file, compute that file’s hash list. The
	//client should then consult the local index file and compare the results, to see whether
	//(1) there are now new files in the base directory that aren’t in the index file, or
	//(2) files that are in the index file, but have changed since the last time the client was executed
	//(i.e., the hash list is different).
	baseDir := client.BaseDir
	//blockSize := client.BlockSize
	//MetaStoreAddr := client.MetaStoreAddr

	// get local hashlist
	localFileInfoMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Fatalf("Error while loading metadata from index.db: %v", err)
	}

	filenames2HashMap := make(map[string][]string)

	// walk through the base directory, cal hash
	err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		// print file name
		// check if it is a file, also not .DS_Store
		if !info.IsDir() && info.Name() != ".DS_Store" && info.Name() != "index.db" {
			fmt.Println("file name: ", info.Name())
			fmt.Println("convert to hash")
			file, err := os.Open(path)
			if err != nil {
				log.Printf("Cannot open file %s: %v", path, err)
				return nil
			}
			defer file.Close()

			// calculate block number
			blocknum := info.Size() / int64(client.BlockSize)
			if info.Size()%int64(client.BlockSize) != 0 {
				blocknum++
			}
			fmt.Println(blocknum)

			// block -> hash list (single file)
			blockHashList := make([]string, blocknum)
			err2, done := blockToHash(path, blocknum, client, file, blockHashList)
			if done {
				return err2
			}
			updateLocalIndexFile(localFileInfoMap, info, blockHashList, baseDir) // compare with local index file
			filenames2HashMap[info.Name()] = blockHashList
		}
		return nil
	})

	// debug
	for k, v := range filenames2HashMap {
		fmt.Println(k, v[0])
	}
	// debug finish
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	fmt.Println("local index: ", localIndex)
}

func updateLocalIndexFile(localFileInfoMap map[string]*FileMetaData, info os.FileInfo, blockHashList []string, baseDir string) {
	if localFileMetaData, ok := localFileInfoMap[info.Name()]; ok {
		if !CompareBlockHashList(localFileMetaData.BlockHashList, blockHashList) {
			// file has changed -> update local index file
			localFileInfoMap[info.Name()] = &FileMetaData{
				Filename: info.Name(),
				Version:  localFileMetaData.Version + 1,
			}
			fmt.Println("File has changed: ", info.Name())
			fmt.Println("Version: ", localFileMetaData.Version+1)
		}
	} else {
		// new file -> update local index file
		localFileInfoMap[info.Name()] = &FileMetaData{
			Filename:      info.Name(),
			Version:       0,
			BlockHashList: blockHashList,
		}
		fmt.Println("New file: ", info.Name())
		fmt.Println("Version: ", 0)
	}

	// mark deleted files
	for filename := range localFileInfoMap {
		filePath := filepath.Join(baseDir, filename)
		_, err := os.Stat(filePath)
		if err != nil {
			// file not exist -> mark as deleted
			localFileInfoMap[filename].Version++
			localFileInfoMap[filename].BlockHashList = make([]string, 0)
			localFileInfoMap[filename].BlockHashList = append(localFileInfoMap[filename].BlockHashList, "0")
			fmt.Println("File deleted: ", filename)
		}
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
		if i == 0 {
			fmt.Println("block hash: ", blockHashList[i])
		}
	}
	return nil, false
}

//	//panic("todo")
//
//	//The client should first scan the base directory, and for each file, compute that file’s hash list. The
//	//client should then consult the local index file and compare the results, to see whether
//	//(1) there are now new files in the base directory that aren’t in the index file, or
//	//(2) files that are in the index file, but have changed since the last time the client was executed
//	//(i.e., the hash list is different).
//
//	baseDir := client.BaseDir
//	blockSize := client.BlockSize
//	MetaStoreAddr := client.MetaStoreAddr
//
//	// get local hashlist
//	localFileInfoMap, err := LoadMetaFromMetaFile(baseDir)
//	if err != nil {
//		log.Fatalf("Error while loading metadata from index.db: %v", err)
//	}
//
//	//compute that file’s hash list
//	err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
//		if !info.IsDir() {
//			filename := info.Name()
//			if filename == DEFAULT_META_FILENAME {
//				return nil
//			}
//			file, err := os.Open(path)
//			if err != nil {
//				log.Printf("Cannot open file %s: %v", path, err)
//				return nil
//			}
//			defer file.Close()
//
//			// calculate block number
//			fileSize := info.Size()
//			blockNum := fileSize / int64(blockSize)
//			if fileSize%int64(blockSize) != 0 {
//				blockNum++
//			}
//
//			// hash block
//			blockHashList := make([]string, blockNum)
//			for i := int64(0); i < blockNum; i++ {
//				block := make([]byte, blockSize)
//				n, err := file.Read(block)
//				if err != nil {
//					log.Printf("Cannot read block %d from file %s: %v", i, path, err)
//					return nil
//				}
//				blockHashList[i] = GetBlockHashString(block[:n])
//			}
//
//			// compare with local index file
//			if localFileMetaData, ok := localFileInfoMap[filename]; ok {
//				if !CompareBlockHashList(localFileMetaData.BlockHashList, blockHashList) {
//					// file has changed -> update local index file
//					localFileInfoMap[filename] = &FileMetaData{
//						Filename: filename,
//						Version:  localFileMetaData.Version + 1,
//					}
//				}
//			} else {
//				// new file -> update local index file
//				localFileInfoMap[filename] = &FileMetaData{
//					Filename:      filename,
//					Version:       0,
//					BlockHashList: blockHashList,
//				}
//			}
//		}
//		return nil
//	})
//	if err != nil {
//		log.Fatalf("Error while walking through the baseDir: %v", err)
//	}
//
//	// mark deleted files
//	for filename := range localFileInfoMap {
//		filePath := filepath.Join(baseDir, filename)
//		_, err := os.Stat(filePath)
//		if err != nil {
//			// file not exist -> mark as deleted
//			localFileInfoMap[filename].Version++
//			localFileInfoMap[filename].BlockHashList = make([]string, 0)
//			localFileInfoMap[filename].BlockHashList = append(localFileInfoMap[filename].BlockHashList, "0")
//		}
//	}
//
//	//Next, the client should connect to the server and download an updated FileInfoMap. For the
//	//purposes of this discussion, let’s call this the “remote index.”
//
//	surfClient := NewSurfstoreRPCClient(MetaStoreAddr, baseDir, blockSize)
//	remoteIndex := make(map[string]*FileMetaData)
//	err = surfClient.GetFileInfoMap(&remoteIndex)
//	if err != nil {
//		log.Fatalf("Error while getting FileInfoMap from the server: %v", err)
//	}
//
//	//The client should now compare the local index (and any changes to local files not reflected in
//	//the local index) with the remote index.
//
//	//First, it is possible that the remote index refers to a file not present in the local index or in the
//	//base directory. In this case, the client should download the blocks associated with that file,
//	//reconstitute that file in the base directory, and then add the updated FileInfo information to the
//	//local index.
//
//	for remoteFilename, remoteFileMetaData := range remoteIndex {
//		localFileMetaData, ok := localFileInfoMap[remoteFilename]
//		_, err = os.Stat(filepath.Join(baseDir, remoteFilename))
//
//		// 1. local index no file
//		// 2. local index with file, but remote is newer
//		// 3. local index with file, but local no file
//		// -> download
//		if !ok || (ok && remoteFileMetaData.Version > localFileMetaData.Version) || err != nil {
//
//			// check if file is deleted
//			if ok && remoteFileMetaData.Version > localFileMetaData.Version {
//				if remoteFileMetaData.BlockHashList[0] == "0" {
//					err = os.Remove(filepath.Join(baseDir, remoteFilename))
//					if err != nil {
//						log.Fatalf("Error while deleting file %s: %v", remoteFilename, err)
//					}
//					// update local index
//					localFileInfoMap[remoteFilename] = remoteFileMetaData
//					continue
//				}
//			}
//
//			// download file
//			blockStoreAddr := ""
//			surfClient.GetBlockStoreAddr(&blockStoreAddr)
//			for _, blockHash := range remoteFileMetaData.BlockHashList {
//				if err != nil {
//					log.Fatalf("Error while getting block %s from the server: %v", blockHash, err)
//				}
//				// get file to local base directory
//				localPath := filepath.Join(client.BaseDir, remoteFilename)
//				localFile, err := os.Create(localPath)
//				if err != nil {
//					log.Fatalf("Cannot create file %s: %v", localPath, err)
//				}
//				defer localFile.Close()
//				// sync write block to file
//				var block Block
//				err = surfClient.GetBlock(blockHash, blockStoreAddr, &block)
//				if err != nil {
//					log.Fatalf("Error while getting block %s from the server: %v", blockHash, err)
//				}
//				_, err = localFile.Write(block.BlockData)
//			}
//			// update local index
//			localFileInfoMap[remoteFilename] = remoteFileMetaData
//		}
//	}
//
//	//Next, it is possible that there are new files in the local base directory that aren’t in the local index
//	//or in the remote index. The client should upload the blocks corresponding to this file to the
//	//server, then update the server with the new FileInfo. If that update is successful, then the client
//	//should update its local index. Note it is possible that while this operation is in progress, some
//	//other client makes it to the server first, and creates the file first. In that case, the UpdateFile()
//	//operation will fail with a version error, and the client should handle this conflict as described in
//	//the next section.
//
//	for localFilename, localFileMetaData := range localFileInfoMap {
//		_, ok := remoteIndex[localFilename]
//		if !ok || remoteIndex[localFilename].Version < localFileMetaData.Version {
//			// upload file
//			blockStoreAddr := ""
//			surfClient.GetBlockStoreAddr(&blockStoreAddr)
//			for _, blockHash := range localFileMetaData.BlockHashList {
//				var block Block
//				var success bool
//				err = surfClient.PutBlock(&block, blockStoreAddr, &success)
//				if err != nil {
//					log.Fatalf("Error while putting block %s to the server: %v", blockHash, err)
//				}
//				// handle conflict
//				if !success {
//					fmt.Println("Conflict: file has been updated by another client")
//					// get updated remote index
//					err = surfClient.GetFileInfoMap(&remoteIndex)
//					if err != nil {
//						log.Fatalf("Error while getting FileInfoMap from the server: %v", err)
//					}
//					// download the updated file
//					remoteFileMetaData := remoteIndex[localFilename]
//					remoteFilename := remoteFileMetaData.Filename
//					blockStoreAddr := ""
//					surfClient.GetBlockStoreAddr(&blockStoreAddr)
//					for _, blockHash := range remoteFileMetaData.BlockHashList {
//						if err != nil {
//							log.Fatalf("Error while getting block %s from the server: %v", blockHash, err)
//						}
//						// get file to local base directory
//						localPath := filepath.Join(client.BaseDir, remoteFilename)
//						localFile, err := os.Create(localPath)
//						if err != nil {
//							log.Fatalf("Cannot create file %s: %v", localPath, err)
//						}
//						defer localFile.Close()
//						// sync write block to file
//						var block Block
//						err = surfClient.GetBlock(blockHash, blockStoreAddr, &block)
//						if err != nil {
//							log.Fatalf("Error while getting block %s from the server: %v", blockHash, err)
//						}
//						_, err = localFile.Write(block.BlockData)
//					}
//					// update local index
//					localFileInfoMap[remoteFilename] = remoteFileMetaData
//				}
//			}
//			// update server
//			err = surfClient.UpdateFile(localFileMetaData, &localFileMetaData.Version)
//			if err != nil {
//				log.Fatalf("Error while updating file %s to the server: %v", localFilename, err)
//			}
//		}
//	}
//}
//
