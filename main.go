package main

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
)

const (
	ModeCompress   = true
	ModeDecompress = false
	BlockSize      = 1024 * 1024
)

type DataBlock struct {
	data  []byte
	index int32
}

func main() {
	args := os.Args[1:]
	if len(args) != 3 {
		fmt.Println("Usage: gziptest [-compress|-decompress] [source file] [target file]")
		return
	}

	switch args[0] {
	case "-compress":
		compressFile(args[1], args[2], ModeCompress)
		fmt.Println("Compressed: " + args[1])
	case "-decompress":
		compressFile(args[1], args[2], ModeDecompress)
		fmt.Println("Decompressed: " + args[1])
	default:
		fmt.Println("Unknown command: " + args[0])
	}
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func compressData(data []byte) []byte {
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)

	_, err := writer.Write(data)
	checkError(err)

	err = writer.Close()
	checkError(err)

	return output.Bytes()
}

func decompressData(data []byte) []byte {
	input := bytes.NewReader(data)
	reader, err := gzip.NewReader(input)
	checkError(err)
	defer reader.Close()

	output, err := io.ReadAll(reader)
	checkError(err)
	return output
}

func compressFile(inFilePath string, outFilePath string, mode bool) {
	threadsCount := runtime.NumCPU()
	inDataBlocks := make(chan DataBlock, threadsCount)
	outDataBlocks := make(chan DataBlock, threadsCount)

	var wgReadData, wgModifyData, wgWriteData sync.WaitGroup
	wgReadData.Add(1)
	wgModifyData.Add(threadsCount)
	wgWriteData.Add(1)

	for i := 0; i < threadsCount; i++ {
		go modifyBlocks(inDataBlocks, outDataBlocks, mode, &wgModifyData)
	}

	go readBlocks(inFilePath, inDataBlocks, mode, &wgReadData)
	go writeBlocks(outDataBlocks, outFilePath, mode, &wgWriteData)

	wgReadData.Wait()
	close(inDataBlocks)

	wgModifyData.Wait()
	close(outDataBlocks)

	wgWriteData.Wait()
}

func readBlocks(inFilePath string, inDataBlocks chan DataBlock, mode bool, wg *sync.WaitGroup) {
	defer wg.Done()

	inFile, err := os.OpenFile(inFilePath, os.O_RDONLY, 0)
	checkError(err)
	defer inFile.Close()

	for blockIndex := 0; ; blockIndex++ {
		var origBlockIndex int32
		var data []byte

		if mode == ModeCompress {
			origBlockIndex = int32(blockIndex)
			data = make([]byte, BlockSize)
		} else {
			err := binary.Read(inFile, binary.LittleEndian, &origBlockIndex)
			if err == io.EOF {
				break
			} else {
				checkError(err)
			}

			var origBlockSize int32
			err = binary.Read(inFile, binary.LittleEndian, &origBlockSize)
			checkError(err)

			data = make([]byte, origBlockSize)
		}

		bytesCount, err := io.ReadFull(inFile, data)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			checkError(err)
		}

		if bytesCount == 0 {
			break
		}

		inDataBlocks <- DataBlock{data[:bytesCount], origBlockIndex}
	}
}

func modifyBlocks(inDataBlocks chan DataBlock, outDataBlocks chan DataBlock, mode bool, wg *sync.WaitGroup) {
	defer wg.Done()

	for dataBlock := range inDataBlocks {
		if mode == ModeCompress {
			dataBlock.data = compressData(dataBlock.data)
		} else {
			dataBlock.data = decompressData(dataBlock.data)
		}

		outDataBlocks <- dataBlock
	}
}

func writeBlocks(outDataBlocks chan DataBlock, outFilePath string, mode bool, wg *sync.WaitGroup) {
	defer wg.Done()

	outFile, err := os.OpenFile(outFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 666)
	checkError(err)
	defer outFile.Close()

	for dataBlock := range outDataBlocks {
		if mode == ModeCompress {
			err := binary.Write(outFile, binary.LittleEndian, int32(dataBlock.index))
			checkError(err)

			err = binary.Write(outFile, binary.LittleEndian, int32(len(dataBlock.data)))
			checkError(err)

			_, err = outFile.Write(dataBlock.data)
			checkError(err)
		} else {
			offset := int64(dataBlock.index) * BlockSize
			_, err = outFile.WriteAt(dataBlock.data, offset)
			checkError(err)
		}
	}
}
