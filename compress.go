package main

import (
	"bytes"
	"compress/gzip"
	"io"
)

func compress(data []byte) []byte {
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)

	checkErr2(writer.Write(data))
	checkErr1(writer.Close())

	return output.Bytes()
}

func decompress(data []byte) []byte {
	input := bytes.NewReader(data)
	reader := checkErr2(gzip.NewReader(input))
	defer checkErr1(reader.Close())

	return checkErr2(io.ReadAll(reader))
}
