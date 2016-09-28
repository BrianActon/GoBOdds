package dummydata

import (
	"fmt"
	"log"
	"database/sql"
	//	"database/sql/driver"	
	//	"flag"
	_ "github.com/denisenkom/go-mssqldb"
)

type voter struct {
	ID 			int
	Voted 		byte
	Surname		string
	FirstName	string
	Initials	[]byte
	RVW			string		//  registered voting ward
	Area 		string
}	

var votersRoll = make([]voter, 1)
var flint int = 0
//-------------------------------------------------------
//
//	addVoter method
//
//-------------------------------------------------------
func (v *voter) addVoter(pv *voter, db *sql.DB) {  //

	if v.ID == 1000000 {
		v.ID		=  	nextVoterID(v.ID)
	} else {
		v.ID		=  	nextVoterID(pv.ID)
	}

	v.Surname 		= 	nextVoterSurname(pv.Surname)
	v.FirstName 	= 	nextVoterFirstName(pv.FirstName)
	v.Initials 		= 	nextVoterInitials(pv.Initials)
	v.RVW 			= 	nextVoterRVW(pv.RVW, len(votersRoll))
	v.Area 			= 	nextVoterArea(pv.Area)	
	
	votersRoll = append(votersRoll, *v)

	// insert using ?
	//------------------------------------------------------------------------------
	//
	// - db.Exec() : INSERT data from a struct into the table
	//
	//------------------------------------------------------------------------------

    _, err := db.Exec("INSERT INTO [Test_Go].[dbo].[VotersRoll] " + 
    				" ([ID],[Voted],[Surname],[FirstName],[Initials],[Registered_Voting_Ward],[Area]) " +
    				" VALUES (?, ?, ?, ?, ?, ?, ?) ", 
    				 v.ID, 0, v.Surname , v.FirstName, v.Initials, v.RVW, v.Area)

	if err != nil {
		fmt.Println("Error INSERTing into [Test_Go].[dbo].[VotersRoll] : ")
	    fmt.Println("err: ", err)
		errorString := err.Error()
	    fmt.Println("errorString: ", errorString)
        return 
    } 
}

//
//
//
//
func LoadDummyData(db *sql.DB) ( error ) {
	fmt.Println("-LoadDummyData-")


    _, err := db.Exec("TRUNCATE TABLE Test_Go.dbo.VotersRoll")

	if err != nil {
		fmt.Println("Error truncating [Test_Go].[dbo].[VotersRoll] : ")
	    fmt.Println("err: ", err)
		errorString := err.Error()
	    fmt.Println("errorString: ", errorString)
        return  err
    } else {
    	fmt.Println("[Test_Go].[dbo].[VotersRoll] successfully truncated")
    }


	// test if truncate worked
	//------------------------------------------------------------------------------
	//
	// - db.QueryRow()  :  for queries where only 1 row is expected to be returned
	//
	//------------------------------------------------------------------------------

	var rowsExist int
	err = db.QueryRow("SELECT COUNT(1) from [Test_Go].[dbo].[VotersRoll]").Scan(&rowsExist)

	switch {
    	case err == sql.ErrNoRows:
            log.Printf("No entries")
    	case err != nil:
            log.Fatal(err)
    	default:
            fmt.Printf("rows = %d\n", rowsExist)
    }

	//	defer dummyExists.Close()

	//  ..then create fresh batch of voters
	if rowsExist != 1 {
		err := createDummyDate(db)
		if err != nil {
			fmt.Println("Error: ", err)
		}
	} 
	return err
}

//
//
//
//
func createDummyDate(db *sql.DB) (error) {
	fmt.Println("-createDummyDate-")

//	votersRoll := make([]voter, 1)

	// starting point ID: 1 million
	storeID := 1000000
	var vST voter 
	// make a voter
	var v voter
	j := 0

	for i := 0; i < 500; i++ {
		// if not first occurence, then use previous occurence data
		if i > 0 {
			j = i - 1
			v.addVoter(&vST, db)  //
		} else {
			j 		= 	i
			v.ID 	=  	storeID 
			v.addVoter(&votersRoll[j], db)  //
		}
		vST = v
	}

/*	for _, v := range votersRoll {
		fmt.Println(" test ", v)
	}
*/
	return nil
}

//
//
//
//
func nextVoterID(i int) (int) {
//	fmt.Println("xx-nextVoterID-", i)
	i++
//	fmt.Println("xx-nextVoterID-", i)
	return i 
} 


//
//
//
//
func nextVoterSurname(s string) (string) {
//	fmt.Println("-nextVoterSurname-")
	s = "bobbity"
	return s
} 

//
//
//
//
func nextVoterFirstName(s string) (string) {
//	fmt.Println("-nextVoterFirstName-")
	s = "bibbity"
	return s
} 


//
//
//
//
func nextVoterInitials(b []byte) ([]byte) {
//	fmt.Println("-nextVoterInitials-")
	
	if b == nil {
		b = append(b, 'A')
//		fmt.Println("A")
	}
	if b[0] == 'C' {
		b[0] = 'B'
//		fmt.Println("B")
	} else {
		b[0] = 'C'
//		fmt.Println("C")
	}
	
//	fmt.Println("-nextVoterInitials exit-", b, b[0], string(b))
	return b
} 

//
//
//
//
func nextVoterRVW(s string, x int) (string) {
//	fmt.Println("-nextVoterRVW-")
	s = fmt.Sprintln(x + 1)
	return s
} 


//
//
//
//
func nextVoterArea(s string) (string) {
//	fmt.Println("-nextVoterArea-")
	s = "dummyArea"
	return s
} 