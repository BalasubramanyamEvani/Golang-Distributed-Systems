// Tests for SquarerImpl. Students should write their code in this file.

package p0partB

import (
	"fmt"
	"testing"
	"time"
)

const (
	timeoutMillis = 5000
)

func TestBasicCorrectness(t *testing.T) {
	fmt.Println("Running TestBasicCorrectness.")
	input := make(chan int)
	sq := SquarerImpl{}
	squares := sq.Initialize(input)
	go func() {
		input <- 2
	}()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
		t.Error("Test timed out.")
	case result := <-squares:
		if result != 4 {
			t.Error("Error, got result", result, ", expected 4 (=2^2).")
		}
	}
}

// Checks if the order of squared
// elements is maintained by sending multiple input requests.
// If not then there is a possible race condition
func TestYourFirstGoTest1(t *testing.T) {
	// Create an input channel for the squarer
	// and initialze
	input := make(chan int, 8)
	squarer := SquarerImpl{}
	output := squarer.Initialize(input)

	// A go routine which sends the val
	// to be squared to squarer
	go func() {
		for i := 0; i < 10; i++ {
			input <- i
		}
	}()

	// Splice to contain result after squaring
	res := []int{}

	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	timeoutReceived := false
	// Loops 10 times to collect output result
	for {
		select {
		// Checks for timeout
		case <-timeoutChan:
			timeoutReceived = true
			break
		// Receive output from squarer
		case result := <-output:
			res = append(res, result)
		}
		if timeoutReceived {
			break
		}
	}

	// Checks whether the result order was maintained or not
	for idx, val := range res {
		expected := idx * idx
		if val != expected {
			t.Error("Possible Race, incorrect result", val, ", expected", expected)
		}
	}
}

// Checks if after
// sending the close signal, the squarer
// stops processing or not. Ideally it should
func TestYourFirstGoTest2(t *testing.T) {
	// Create an input channel for the squarer
	// and initialze
	input := make(chan int, 8)
	squarer := SquarerImpl{}
	output := squarer.Initialize(input)

	// A go routine which sends the val
	// to be squared to squarer
	go func() {
		for i := 0; i < 10; i++ {
			input <- i
		}
	}()

	// Splice to contain result after squaring
	res := []int{}

	// timeout channel when the squarer stops
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)

	timeoutReceived := false
	i := 0

	// Loops 10 times to collect output result
	for {
		select {
		// Checks for timeout
		case <-timeoutChan:
			timeoutReceived = true
			break
		// Receive output from squarer
		case result := <-output:
			res = append(res, result)
		}
		// In between close the squarer
		if i == 5 {
			squarer.Close()
		}
		if timeoutReceived {
			break
		}
		i += 1
	}

	// Checks whether the length of the result is less than expected
	if len(res) == 10 {
		t.Error("Close didn't work as expected, length of the result should have been less than 10")
	}
}
