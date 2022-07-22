package main

func checkErr1(err error) {
	if err != nil {
		panic(err)
	}
}

func checkErr1Lazy(f func() error) {
	err := f()
	if err != nil {
		panic(err)
	}
}

func checkErr2[T any](v T, err error) T {
	checkErr1(err)
	return v
}
