package main //voters_roll

import (
	"fmt"
	"database/sql"
	//	"database/sql/driver"	
	//	"flag"
	"voters_roll/dummydata"
	_ "github.com/denisenkom/go-mssqldb"
)

var votersChoice = make(chan string)


//
//
//  
//
func main() {

	fmt.Println("Program start...")
	//   	create connection to SQL DB voter_roll
	
	// log in to server
 	db, err := sql.Open("mssql", dsn)
	if err != nil {
		message := "Cannot connect - 1 : "
		fmt.Println(message)
		errorString := err.Error()
		fmt.Println(errorString)
		return
	}
	// ping to test connection
	err = db.Ping()
	if err != nil {
		message := "Ping failure - 2 : "
		fmt.Println(message)
		errorString := err.Error()
		fmt.Println(errorString)
		return
	}
	defer db.Close()


	// 		Generate 1 million voters
	err =	dummydata.LoadDummyData(db)
	if err != nil {
		fmt.Println("Load dummy data fail!!!!")
		errorString := err.Error()
		fmt.Println(errorString)
		return
	}

	candidates, err := prepVotingTable(db)
	if err != nil {
		fmt.Println("Prepping Voting table fail!!!!")
		errorString := err.Error()
		fmt.Println(errorString)
		return
	}

	//		Let them vote
	err = vote(db, candidates)
	if err != nil {
		fmt.Println("Vote process fail!!!!")
		errorString := err.Error()
		fmt.Println(errorString)
		return
	}

	fmt.Println("...The End!")

}

//
//
//	start voting randomly using concurrency
//
func vote(db *sql.DB, candidates []string) (error) {
	fmt.Println("...Time to vote...")

	// done for when there are no more voters
    done := make(chan struct{})
    for _, l := range candidates { go myVote(db, l, done) }
    for _ = range langs { <-done }

	return nil
}

func myVote(db *sql.DB, name string, done chan struct{}) {
    select {
    case opponent := <-battle:
        fmt.Printf("%s beat %s\n", name, opponent)
    case battle <- name:
        // I lost :-(
    }
    done <- struct{}{}
}

//
//
//  
//
func prepVotingTable(db *sql.DB) ([]string, error)  {
	fmt.Println("...Prep the candidates...")

	candidates := make([]string, 1)
	// Truncate
    _, err := db.Exec("TRUNCATE TABLE Test_Go.dbo.Votes")
	if err != nil {
		message := "Error truncating [Test_Go].[dbo].[Votes] : "
		fmt.Println(message)
	    fmt.Println("err: ", err)
		errorString := err.Error()
	    fmt.Println("errorString: ", errorString)
        return candidates, err
    } else {
    	fmt.Println("[Test_Go].[dbo].[Votes] successfully truncated")
    }


    _, err = db.Exec("INSERT INTO [Test_Go].[dbo].[Votes] ([Candidate], [tally]) VALUES ('Frump', 0) ")
	if err != nil {
		message := "Error INSERTing into [Test_Go].[dbo].[Votes] : "
		fmt.Println(message)
	    fmt.Println("err: ", err)
		errorString := err.Error()
	    fmt.Println("errorString: ", errorString)
        return candidates, err
    } else {
		candidates = append(candidates, "Frump")
    	fmt.Println("Candidate Frump inserted")
    }

    _, err = db.Exec("INSERT INTO [Test_Go].[dbo].[Votes] ([Candidate], [tally]) VALUES ('Cnuz', 0) ")
	if err != nil {
		message := "Error INSERTing into [Test_Go].[dbo].[Votes] : "
		fmt.Println(message)
	    fmt.Println("err: ", err)
		errorString := err.Error()
	    fmt.Println("errorString: ", errorString)
        return candidates, err
    } else {
		candidates = append(candidates, "Cnuz")
    	fmt.Println("Candidate Cnuz inserted")
    }

    _, err = db.Exec("INSERT INTO [Test_Go].[dbo].[Votes] ([Candidate], [tally]) VALUES ('Slanders', 0) ")
	if err != nil {
		message := "Error INSERTing into [Test_Go].[dbo].[Votes] : "
		fmt.Println(message)
	    fmt.Println("err: ", err)
		errorString := err.Error()
	    fmt.Println("errorString: ", errorString)
        return candidates, err
    } else {
		candidates = append(candidates, "Slanders")
    	fmt.Println("Candidate Slanders inserted")
    }

    _, err = db.Exec("INSERT INTO [Test_Go].[dbo].[Votes] ([Candidate], [tally]) VALUES ('Clifton', 0) ")
	if err != nil {
		message := "Error INSERTing into [Test_Go].[dbo].[Votes] : "
		fmt.Println(message)
	    fmt.Println("err: ", err)
		errorString := err.Error()
	    fmt.Println("errorString: ", errorString)
        return candidates, err
    } else {
		candidates = append(candidates, "Clifton")
    	fmt.Println("Candidate Clifton inserted")
    }

    return candidates, nil
}