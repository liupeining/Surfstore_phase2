package surfstore

import (
	context "context"
	"database/sql"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"os"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RPCClient struct {
	MetaStoreAddr string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	//panic("todo")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = b.Hashes
	return conn.Close()
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	// grpc.withTransportCredentials: use insecure credentials, meaning no encryption
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	// conn: to the block store server
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	// no return value, set the block data in the input block
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	// conn: to the block store server
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block) // actually I don't know why we need this success
	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	//panic("todo")
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	// conn: to the block store server
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	//message BlockHashes {
	//    repeated string hashes = 1;
	//}
	b, err := c.MissingBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = b.Hashes
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	//panic("todo")
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	// conn: to the meta store server
	c := NewMetaStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	m, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	// fmt.Println("m: ", m)
	// I don't know why in project4, multiple blockserver, m can be nil
	if err != nil {
		conn.Close()
	}
	*serverFileInfoMap = m.FileInfoMap
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	//panic("todo")
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	// conn: to the meta store server
	c := NewMetaStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m, err := c.UpdateFile(ctx, fileMetaData)
	*latestVersion = m.Version
	if err != nil {
		conn.Close()
		return err
	}
	// close the connection
	return conn.Close()
}

//func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
//	//panic("todo")
//	// connect to the server
//	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
//	if err != nil {
//		return err
//	}
//	// conn: to the meta store server
//	c := NewMetaStoreClient(conn)
//	// perform the call
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	m, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
//	*blockStoreAddr = m.Addr
//	if err != nil {
//		conn.Close()
//	}
//	// close the connection
//	return conn.Close()
//}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	//panic("todo")
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	// conn: to the meta store server
	c := NewMetaStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockStoreMaptemp, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	for k, v := range blockStoreMaptemp.BlockStoreMap {
		(*blockStoreMap)[k] = v.Hashes
	}
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	//panic("todo")
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	// conn: to the meta store server
	c := NewMetaStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddrs = m.BlockStoreAddrs
	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {
	path := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		indexFile, err := os.Create(path)
		if err != nil {
			log.Fatal("Error During creating file: ", err)
		}
		indexFile.Close()
		db, err := sql.Open("sqlite3", path)
		defer db.Close()
		if err != nil {
			log.Fatal("Error during opening index.db file", err)
		}
		statement, err := db.Prepare(createTable)
		if err != nil {
			log.Fatal("cannot create table", err)
		}
		statement.Exec()
		statement.Close()
	}
	return RPCClient{
		MetaStoreAddr: hostPort,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
