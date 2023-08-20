package mr

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"unicode"
)

func TestSort(t *testing.T) {
	value1 := KeyValue{Key: "act", Value: "1"}
	value2 := KeyValue{Key: "abou", Value: "1"}
	value3 := KeyValue{Key: "asct", Value: "1"}

	key := ByKey{value1, value2, value3}
	sort.Sort(key)

	fmt.Println(key)
}

func TestWorker(t *testing.T) {
	_, err := MockCoordinator("/Users/chenmengshu/Code/6.5840/src/main", 10)
	if err != nil {
		t.Fatal(err)
	}

	mapfunc := func(filename string, contents string) []KeyValue {
		// function to detect word separators.
		ff := func(r rune) bool { return !unicode.IsLetter(r) }

		// split contents into an array of words.
		words := strings.FieldsFunc(contents, ff)

		kva := []KeyValue{}
		for _, w := range words {
			kv := KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}

	reducefunc := func(key string, values []string) string {
		// return the number of occurrences of this word.
		return strconv.Itoa(len(values))
	}

	Worker(mapfunc, reducefunc)

}

func TestSignalJobFinished(t *testing.T) {
	master, err := MockCoordinator("/Users/chenmengshu/Code/6.5840/src/main", 10)
	if err != nil {
		t.Fatal(err)
	}
	originLen := len(master.JobList)

	job := AskForJob()
	SignalJobFinished(*job)

	if len(master.JobList) != originLen-1 {
		t.Fatal("Signal Job finished error failed ...")
	}
}

func TestAskForR(t *testing.T) {
	_, err := MockCoordinator("/Users/chenmengshu/Code/6.5840/src/main", 8)
	if err != nil {
		t.Fatal(err)
	}

	r := AskForR()
	if r != 8 {
		t.Fatalf("Wrong nReduce number: %d", r)
	}
}

func TestAskForJob(t *testing.T) {
	_, err := MockCoordinator("/Users/chenmengshu/Code/6.5840/src/main", 8)
	if err != nil {
		t.Fatal(err)
	}

	job := AskForJob()
	if job == nil {
		t.Fatal("Ask for job failed")
	}
}
