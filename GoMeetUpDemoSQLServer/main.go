package main //GoMeetUpDemoSQLServer

import(
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
	
	_ "github.com/denisenkom/go-mssqldb"

	"voters_roll/dummydata"
)

type credentials struct {
	server 		string 
	userID 		string 
	password 	string 
	database	string 
}


// --Working
//******************************************************************************************
//
//  Load database login credentials from config file
//
//******************************************************************************************
func (c *credentials)  loadCredentials()  {

	f, err := os.Open("loadCredentials.cnfg")
	
	check(err)

	input := bufio.NewScanner(f)
	
	i := 0

	for input.Scan() {

		switch i {
			case 0 : 
				c.server 	= input.Text()
			case 1 : 
				c.userID 	= input.Text()
			case 2 : 
				c.password 	= input.Text()
			case 3 : 
				c.database 	= input.Text()
			default :	
				fmt.Println("a line too many...")	
		}

		i++		
	}
	
	return 
}


// --Working
//******************************************************************************************
//
//  Load database login credentials from config file
//
//******************************************************************************************
func main() {

	fmt.Println("Go!")

	t := time.Now().Format(time.RFC850)
	fmt.Println("Run date start : ", t)

	cpus := runtime.NumCPU()
	x  	 := runtime.GOMAXPROCS(cpus - 1)

	fmt.Println("maxcpu = ", x)
	fmt.Println("CPU used", runtime.GOMAXPROCS(-1))

	//-------------------------------------------------
	// log in to SQL server 
	//-------------------------------------------------
	//   - sql.Open() : Get a connection to mssql (db)
	//	 - db.Ping()  : Test the connection created
	//   - db.Close() : Close connection when finished.
	//-------------------------------------------------

	var c credentials
	c.loadCredentials()

    dsn := "server=" + c.server + ";user id=" + c.userID + ";password=" + c.password + ";database=" + c.database


	// sql.Open()

	db, err := sql.Open("mssql", dsn)

	if err != nil {
		fmt.Println("Cannot connect - 1 : ")
		errorString := err.Error()
		fmt.Println(errorString)
		return
	} else {
		fmt.Println("Opened")
	}


	// db.Ping()

	err = db.Ping()

	if err != nil {
		fmt.Println("Ping failure - 2 : ")
		errorString := err.Error()
		fmt.Println(errorString)
		return
	} else {
		fmt.Println("Ping successfull!")
	}

	// db.Close()

	defer db.Close()


	//-------------------------------------------------
	// 	Generate 1 million voters
	//-------------------------------------------------
	err =	dummydata.LoadDummyData(db)
	
	if err != nil {
		fmt.Println("Load dummy data fail!!!!")
		errorString := err.Error()
		fmt.Println(errorString)
		return
	}


	//-------------------------------------------------
	//  load candidates : demo db.Exec
	//-------------------------------------------------
	candidates, err := prepVotingTable(db)

	if err != nil {
		fmt.Println("Prepping Voting table fail!!!!")
		errorString := err.Error()
		fmt.Println(errorString)
		return
	} else {
		fmt.Println("a: ", candidates)
	}


	//-------------------------------------------------
	//	Let them vote
	//-------------------------------------------------
	err = vote(db, candidates)

	if err != nil {
		fmt.Println("Vote process fail!!!!")
		errorString := err.Error()
		fmt.Println(errorString)
		return
	}


	//-------------------------------------------------
	//	Produce results from voting
	//-------------------------------------------------
/*
	err = showResultsofVoting()

	if err != nil {
		fmt.Println("Print vote results fail!!!!")
		errorString := err.Error()
		fmt.Println(errorString)
		return
	}

*/
	t1 := time.Now().Format(time.RFC850)

	fmt.Println("Run date start : ", t)
	fmt.Println("Run date end 	: ", t1)

	fmt.Println("The End!")
}


// -- 
//******************************************************************************************
//
//  create a chan for our goroutines
//
//******************************************************************************************
var chan_votingBooth = make(chan string, 250)


// -- 
//******************************************************************************************
//
//  Everybodies vote is secret!
//
//******************************************************************************************
func vote(db *sql.DB, candidates []string) (error)  {
	//-------------------------------------------------
	// 
	//  Demo db.Query
	//                 
	//-------------------------------------------------

	rows, err := db.Query("SELECT ID from [dbo].[VotersRoll]")

	storeRows := make([]string, 0)

	if err != nil {
		fmt.Println("Query error: ", err)
	}
	
	defer rows.Close()

    chan_done := make(chan struct{}, 250)

    
    var ID string
    var wg sync.WaitGroup

    for rows.Next() { 
    	err := rows.Scan(&ID)

    	if err != nil {
			fmt.Println("rows.Next error: ", err)
		}

    	wg.Add(1)

    	go func(ID string) {
    		defer wg.Done()
    		votingStation(db, ID, chan_done) 
    	}(ID)	
    	storeRows = append(storeRows, ID)
    }
    

    for _ = range storeRows { <-chan_done }

    go func() {
    	wg.Wait()    
    	close(chan_done)
    }()
	return nil
}


// -- 
//******************************************************************************************
//
//  Everybodies vote is secret!
//
//******************************************************************************************
func votingStation(db *sql.DB, ID string, chan_done chan struct{}) {
//	fmt.Println("ID: ", ID)

	// 1. add 1 to which ever candidate gets the vote
	// 2. Update voters record to show proof of vote

	select {

    	case trump := <-chan_votingBooth:
    		err := markTrump(db, trump)
    		if err != nil { fmt.Println("markTrump fail")}
    		check(err)

	    case clinton := <-chan_votingBooth:
    		err := markClinton(db, clinton)
    		if err != nil { fmt.Println("markClinton fail")}
        	check(err)

	    case johnson := <-chan_votingBooth:
    		err := markJohnson(db, johnson)
    		if err != nil { fmt.Println("markJohnson fail")}
        	check(err)

	    case spoiler := <-chan_votingBooth:
    		err := markSpoiler(db, spoiler)
    		if err != nil { fmt.Println("markSpoiler fail")}
        	check(err)

	    case chan_votingBooth <- ID:
        
    }


    chan_done <- struct{}{}

}

// -- 
//******************************************************************************************
//
//  Everybodies vote is secret!
//
//******************************************************************************************
func markTrump(db *sql.DB, name string) (error){

	_, err := db.Exec("EXEC	[Test_Go].[dbo].[SP_Add_Trump] ")

	if err != nil {
			fmt.Println("rows.Query(SP_Add_Trump) error: ", err)
		}

	return err
}

// -- 
//******************************************************************************************
//
//  Everybodies vote is secret!
//
//******************************************************************************************
func markClinton(db *sql.DB, name string) (error){

	_, err := db.Query("EXEC [Test_Go].[dbo].[SP_Add_Clinton] " )

	if err != nil {
			fmt.Println("rows.Query(SP_Add_Clinton) error: ", err)
		}

	return err
}

// -- 
//******************************************************************************************
//
//  Everybodies vote is secret!
//
//******************************************************************************************
func markJohnson(db *sql.DB, name string) (error){

	_, err := db.Exec("EXEC	[Test_Go].[dbo].[SP_Add_Johnson] ")

	if err != nil {
			fmt.Println("rows.Query(SP_Add_Johnson) error: ", err)
		}

	return err
}

// -- 
//******************************************************************************************
//
//  Everybodies vote is secret!
//
//******************************************************************************************
func markSpoiler(db *sql.DB, name string) (error){
	
	_, err := db.Exec("EXEC	[Test_Go].[dbo].[SP_Add_Spoiled] " )

	if err != nil {
			fmt.Println("rows.Query(SP_Add_Spoiler) error: ", err)
		}

	return err
}

// -- Working
//******************************************************************************************
//
//  db.Exec
//
//******************************************************************************************
func prepVotingTable(db *sql.DB) ([]string, error)  {

	//-------------------------------------------------
	// log in to SQL server 
	//-------------------------------------------------
	//   - db.Exec() : issues commands 
	//                 eg: TRUNCATE, INSERT
	//-------------------------------------------------
	fmt.Println("...Prep the candidates...")

	candidates := []string{"Trump", "Clinton", "Johnson", "spoil"}


	// db.Exec()  -  TRUNCATE

	_, err := db.Exec("TRUNCATE TABLE Test_Go.dbo.Votes")

	if err != nil {
		fmt.Println("Error truncating [Test_Go].[dbo].[Votes] : ")
	   	fmt.Println("err: ", err)
		errorString := err.Error()
	   	fmt.Println("errorString: ", errorString)
       	return candidates, err
    } else {
    	fmt.Println("[Test_Go].[dbo].[Votes] successfully truncated")
    }
    

	// db.Exec()  -  INSERT

	for i := 0; i < 4; i++ {
	
    	_, err = db.Exec("INSERT INTO [Test_Go].[dbo].[Votes] ([Candidate], [tally]) VALUES ( ? , 0) ", candidates[i])

		if err != nil {
			fmt.Println("Error INSERTing into [Test_Go].[dbo].[Votes] : ")
	    	fmt.Println("err: ", err)
			errorString := err.Error()
	    	fmt.Println("errorString: ", errorString)
        	return candidates, err
    	} else {
			fmt.Println("Candidate " +  candidates[i] + " inserted")
    	}
    }	

    return candidates, nil

}


// --Working
//******************************************************************************************
//
//  error check
//
//******************************************************************************************

func check(e error) {
    if e != nil {
        panic(e)
    }
}