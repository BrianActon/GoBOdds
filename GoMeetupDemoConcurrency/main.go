package main

import (
	"fmt"
	"os/exec"
	"runtime"
	"sync"
	"time"
)

//***************************************************************************
//
//		Concurrency
//
//***************************************************************************

// 1. GOMAXPROCS

// 2. Concurrency with waitgroups

// 3. Concurrency With Channels
//    Concurrency visualization!!!!  -->  https://divan.github.io/posts/go_concurrency_visualize/

func main() {

	goMaxDone := goMax()
	fmt.Println(goMaxDone)

	fmt.Printf("\n\nDid you notice that we were asked to 'press any key' before the functions had completed their run?\n\n")
	fmt.Printf("Did you notice that one cpu was running a bit lighter than the rest?\n\n")

	wgDone := workGroupDemo()
	fmt.Println(wgDone)

	fmt.Printf("\n\nDid you notice that the numbers are not in sequence!?\n\n")

	vcDone := visualConcurrency()
	fmt.Println(vcDone)

}

//***************************************************************************
//***************************************************************************
//
//	1.	GOMAXPROCS
//
//***************************************************************************
//***************************************************************************
func goMax() string {

	start := time.Now()

	// 2. GOMAXPROCS

	cpus := runtime.NumCPU()
	maxprocs := runtime.GOMAXPROCS(cpus - 1)

	fmt.Printf("\n\n")
	fmt.Printf("Need to import 'runtime'\n")
	fmt.Println("$GOMAXPROCS=", maxprocs)
	fmt.Println("Detected CPUs:", cpus)
	fmt.Println("CPUs to burn:", maxprocs-1)
	fmt.Printf("\n\n")

	fmt.Println("Be aware... Not setting GOMAXPROCS can leave your system a tad clogged...")
	fmt.Println("... ")
	fmt.Printf("\n\n")
	// demo goroutines
	// a goroutine is similar to a thread, just WAY lighter!
	// a goroutine uses up 2kb
	for i := 0; i < maxprocs-1; i++ {
		// this is only kicking off 3 goroutines
		go f()
	}

	var input string
	fmt.Println("press any key to continue...but only after watching the resource monitor for 20 seconds! ")
	fmt.Scanln(&input)

	fmt.Printf("%.8fs elapsed\n", time.Since(start).Seconds())

	return "GOMAXPROCS demo complete..."
}

//***************************************************************************
//  f() only used to pump out endless amount of threads
//***************************************************************************
func f() {
	start := time.Now()
	// infinite loop
	for i := 0; i < 100; i++ {
		i = 2
		if time.Since(start).Seconds() > 20 {
			fmt.Println("um..", time.Since(start).Seconds())
			break
		}

	}

}

//***************************************************************************
//***************************************************************************
//
//	2.	Workgroups
//
//	- Add()
//	- Done()
//	- Wait()
//
//***************************************************************************
//***************************************************************************
func workGroupDemo() string {

	fmt.Println(" ")
	fmt.Println("You need to import 'sync' to use wait groups")
	fmt.Println(" ")
	fmt.Println("    1. Create a variable for your workgroup:  var wg sync.WaitGroup ")
	fmt.Println("    2. Increment for each goroutine created:  wg.Add(1)")
	fmt.Println("    3. when finished doing work		:  wg.Done()")
	fmt.Println("    4. Set up another goroutine to wait for all goroutines to finish running")
	fmt.Println("				:  wg.Wait()")
	fmt.Println(" ")

	numbers := make(chan int)
	var wg sync.WaitGroup //  number of working goroutines  s

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		// worker
		go func(i int) {
			defer wg.Done()
			numbers <- i
		}(i)
	}

	// closer
	go func() {
		wg.Wait()
		close(numbers)
	}()

	i := 0

	for number := range numbers {
		fmt.Printf("%d\t", number)
		if i > 9 {
			fmt.Printf("\n")
			i = 0
		}
		i++
	}

	var input string
	fmt.Printf("\n")
	fmt.Println("press any key to continue...")
	fmt.Scanln(&input)

	return "Workgroups demo complete..."

}

//***************************************************************************
//***************************************************************************
//
//	3.	Concurrency Visualization
//
//***************************************************************************
//***************************************************************************

func visualConcurrency() string {

	start := time.Now()

	var input string
	// 1. Concurrency visualization!

	fmt.Println("Awesome... I say AWESOME Concurrency visualization web page!")
	fmt.Println("... ")
	fmt.Println("press any key to continue... ")
	fmt.Scanln(&input)

	go exec.Command("C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe", "https://divan.github.io/posts/go_concurrency_visualize/").Run()

	fmt.Println("press any key to continue... ")
	fmt.Scanln(&input)

	fmt.Printf("%.8fs elapsed\n", time.Since(start).Seconds())

	return "Visualization webpage run..."
}
