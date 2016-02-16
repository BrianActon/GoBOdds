package main

import(

	"os"
	//"os/exec"
	"time"
	"io"
	//"io/ioutil"
	"fmt"
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	)

//******************************************************************************************
//  Program runs after M3DailyChecks.exe
//
//  Program Layout
//  --------------
//
//	1.	Get list of RBE vs M3 imbalances from [M3].[dbo].[M3DailyBalances]
//	2.	Create output files with only the imbalances
//	3.-> ?????  -> Add [M3].[dbo].[M3DailyBalances] to [M3].[dbo].[M3DailyBalancesReporting]
//	4.  Build report from [M3].[dbo].[M3DailyBalancesReporting]
//	5.-> ?????  ->   Dump report on "\\p3jhbfs01\Clients\ABSA\27. Rewards\01 - MultiChannel Marketing Manager
//										\03 - ECM Communications\03 - Reporting"
//	
//******************************************************************************************

//******************************************************************************************
//
//  main 
//
//******************************************************************************************
func main() {

	// log in to server
	dsn := "server=" + "localhost" + ";user id=" + "PCUBED\\nuclog" + ";password=" + "P@ssword135" +  ";database=" + "M3"  + ";encrypt=" + "disable"
	db, err := sql.Open("mssql", dsn)
	if err != nil {
		message := "Cannot connect - 1 : "
		fmt.Println(message)
	//	errorString := err.Error()
		return
	}

	// ping to test connection
	err = db.Ping()
	if err != nil {
		message := "Ping failure - 2 : "
		fmt.Println(message)
	//	errorString := err.Error()
		return
	}
	defer db.Close()


    //
    // This function will create the following reports
    // 		- Daily report:  To be found on P3JHBFS01..Reports     
    //		- Daily discrepancies: sent to support team and found on P3ISM3VM/P3ISM3DEV/P3ISM3FOVM
    InRBENotM3Report := createInRBENotM3(db)
    if InRBENotM3Report == "failure" {
    	fmt.Println("InRBENotM3 report not done: ", InRBENotM3Report)
    		fmt.Println("")
    }
    	
    
    InM3NotRBEReport := createInM3NotRBE(db)
    if InM3NotRBEReport == "failure" {
    	fmt.Println("InRBENotM3 report not done: ", InM3NotRBEReport)
    		return
    }	
        	
    fmt.Println("In RBE Not M3 Report done: ", InRBENotM3Report)
    fmt.Println("In M3 Not RBE Report done: ", InM3NotRBEReport)

	return
}	
 

// -- Working
//******************************************************************************************
//
//  Create a file with problem IDs by campaign ID in order to track issues faster
//
//******************************************************************************************
func createInRBENotM3(db *sql.DB) (ret string){
	
	t := time.Now()
	fmt.Println("Run date : ", t)

//	tDate := t.Format("20060102")

	rows, err := db.Query(
		"SELECT [campaignID] ,[SA_ID_Number] " +
			" FROM [M3].[dbo].[campaignAudienceList_tble] " +
			" WHERE (inM3 = 0 and inRBE = 1) " +
	 		" ORDER BY [campaignID] ,[SA_ID_Number] "  )

	if err != nil {
		fmt.Println("Query error: ", err)
		//log.Fatal(err)
	}
	defer rows.Close()

	csvfile, ferr := os.Create("E:\\M3_P3_Admin\\DailyInRBENotM3.csv")
	if ferr != nil {
        fmt.Println("Error:", ferr)
        return "create file error"
    }
    defer csvfile.Close()


	prn := fmt.Sprintln("Campaign ID 	Member ID")	
	_, werr := io.WriteString(csvfile, prn)
	if werr != nil {
		fmt.Println("Error writing to csvfile")
	}
	prn = fmt.Sprintln("***********    ************ ")
	_, werr = io.WriteString(csvfile, prn)
	if werr != nil {
		fmt.Println("Error writing to csvfile")
	}
	prn = fmt.Sprintln(" ")		
	_, werr = io.WriteString(csvfile, prn)
	if werr != nil {
		fmt.Println("Error writing to csvfile")
	}

//	fmt.Println("Campaign ID 	ID")	
//	fmt.Println("***********    ************")
//	fmt.Println(" ")		

	for rows.Next() {
		
		var (
		CampaignID string
		SA_ID_Number string
		)

		err = rows.Scan(&CampaignID, &SA_ID_Number)
		if err != nil {
			fmt.Println("rows.Next error: ", err)
		}
		prn = fmt.Sprintf("%s\t\t%s\n", CampaignID, SA_ID_Number)

		_, werr = io.WriteString(csvfile, prn)
		if werr != nil {
			fmt.Println("Error writing to csvfile")
		}
		
		prn = fmt.Sprintln("-----------------------------------------------------------------------------*")

		_, werr = io.WriteString(csvfile, prn)
		if werr != nil {
			fmt.Println("Error writing to csvfile")
		}

	//	fmt.Printf("%s\t\t%d \n", CampaignID, SA_ID_Number)
	//	fmt.Println("-----------------------------------------------------------------------------*")
	}

	fmt.Println("c")

	err = rows.Err()
	if err != nil {
		fmt.Println("Huh?: ", err)
	}
	
	return
}


// -- Working
//******************************************************************************************
//
//  Create a file with problem IDs by campaign ID in order to track issues faster
//
//******************************************************************************************
func createInM3NotRBE(db *sql.DB) (ret string){
	
	t := time.Now()
	fmt.Println("Run date : ", t)

//	tDate := t.Format("20060102")

	rows, err := db.Query(
		"SELECT [campaignID] ,[SA_ID_Number] " +
			" FROM [M3].[dbo].[campaignAudienceList_tble] " +
			" WHERE (inM3 = 1 and inRBE = 0) " +
	 		" ORDER BY [campaignID] ,[SA_ID_Number] "  )
	if err != nil {
		fmt.Println("Query error: ", err)
	}
	defer rows.Close()

	csvfile, ferr := os.Create("E:\\M3_P3_Admin\\DailyInM3NotRBE.csv")
	if ferr != nil {
        fmt.Println("Error:", ferr)
        return "create file error"
    }
    defer csvfile.Close()


	prn := fmt.Sprintln("Campaign ID 	Member ID")	
	_, werr := io.WriteString(csvfile, prn)
	if werr != nil {
		fmt.Println("Error writing to csvfile")
	}
	prn = fmt.Sprintln("***********    ************ ")
	_, werr = io.WriteString(csvfile, prn)
	if werr != nil {
		fmt.Println("Error writing to csvfile")
	}
	prn = fmt.Sprintln(" ")		
	_, werr = io.WriteString(csvfile, prn)
	if werr != nil {
		fmt.Println("Error writing to csvfile")
	}

//	fmt.Println("Campaign ID 	ID")	
//	fmt.Println("***********    ************")
//	fmt.Println(" ")		

	for rows.Next() {
		
		var (
		CampaignID string
		SA_ID_Number string
		)
		err = rows.Scan(&CampaignID, &SA_ID_Number)
		if err != nil {
			fmt.Println("rows.Next error: ", err)
		}
		prn = fmt.Sprintf("%s\t\t%s\n", CampaignID, SA_ID_Number)

		_, werr = io.WriteString(csvfile, prn)
		if werr != nil {
			fmt.Println("Error writing to csvfile")
		}
		
		prn = fmt.Sprintln("-----------------------------------------------------------------------------*")

		_, werr = io.WriteString(csvfile, prn)
		if werr != nil {
			fmt.Println("Error writing to csvfile")
		}

	//	fmt.Printf("%s\t\t%d \n", CampaignID, SA_ID_Number)
	//	fmt.Println("-----------------------------------------------------------------------------*")
	}

	fmt.Println("c")

	err = rows.Err()
	if err != nil {
		fmt.Println("Huh?: ", err)
		//log.Fatal(err)
	}
	
	return
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