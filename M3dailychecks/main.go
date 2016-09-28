package main

import(

	"os"
	"os/exec"
	"time"
	"io"
	"io/ioutil"
	"fmt"
	"database/sql"
	_ "github.com/denisenkom/go-mssqldb"
	)


//******************************************************************************************
//  Program Layout
//  --------------
//
//	1.	main() gets list of files and then hands list over to SQLProcessing()
//	2.	SQLProcessing() does bulk of the work
//		a. Loads M3 files
//		b. Loads RBE M3_Checks file
//		c. Runs a SQL Query which will do a full comparison
//		d. Create a report highlighting where the imbalances are found
//
//
//******************************************************************************************


//******************************************************************************************
//
//  main gets list of files and then hands list over to SQLProcessing
//
//******************************************************************************************
func main() {


	err := os.Remove("E:\\M3 Export\\M3_ERRORS.Error.txt")
    if err != nil {
    	fmt.Println("Warning: Cant remove M3_ERRORS.Error.txt? Does it exist? Fine if it does not.")
    	fmt.Println("err: ", err)
    }

	err = os.Remove("E:\\M3 Export\\M3_ERRORS")
    if err != nil {
    	fmt.Println("Warning: Cant remove M3_ERRORS? Does it exist? Fine if it does not.")
    	fmt.Println("err: ", err)
    }

	t := time.Now()
	fmt.Println("Run date : ", t)

	fmt.Println("Run day : ", t.Weekday())

//
// 	- Weekday, process daily files
//	- Saturday and Sunday, only load files into tables and stop there
//	- Monday, process all files from weekend
//
	switch t.Weekday() {
	case 0, 7 	: 
		fmt.Println("Weekend - Do nothing")
	default:
		fmt.Println("weekday - Normal processing")

	    files := getFileList()

		message, error := SQLProcessing(files)

		fmt.Println("Last message: ", message)
		fmt.Println("Last error: ", error)
	}	
}


//  --Working!
//******************************************************************************************
//
//  Load all files from M3 into SQL
//
//******************************************************************************************

func SQLProcessing(fileName []string) (message string, errorString string){

	// Load config file to use with logging into server
	//  **  need to write prog to update config file depending on which server is being used.
/*	conf := loadConfig()

	if conf == "" {
		fmt.Println("poor config")
	}  */

	// log in to server
	//dsn := "server=" + "localhost" + ";user id=" + "" + ";password=" + "" +  ";database=" + ""  + ";encrypt=" + "disable"
	db, err := sql.Open("mssql", dsn)
	if err != nil {
		message = "Cannot connect - 1 : "
		errorString = err.Error()
		return
	}

	// ping to test connection
	err = db.Ping()
	if err != nil {
		message = "Ping failure - 2 : "
		errorString = err.Error()
		return
	}
	defer db.Close()

	// test if 'M3.dbo.M3_AllCampaigns' exists, then drop
    _, err = db.Exec("IF OBJECT_ID('M3.dbo.M3_AllCampaigns') IS NOT NULL DROP TABLE M3.dbo.M3_AllCampaigns")
	if err != nil {
		message = "Error dropping M3_AllCampaigns - 3 : "
	    fmt.Println("err x1: ", err)
     //   return 
    } else {
    	fmt.Println("M3_AllCampaigns successfully found and dropped")
    }

    // IF M3_Checks exists, DROP, then create new
    _, err = db.Exec("DROP TABLE M3.dbo.M3_Checks")
	if err != nil {
		message = "Error dropping M3_Checks - 4 : "
	    fmt.Println("err x2: ", err)
        //return 
    } else {
    	fmt.Println("M3_Checks successfully dropped")
    }

	_, err = db.Exec("CREATE TABLE [M3].[dbo].[M3_Checks] "+
						"( Period	DATE, SA_ID_Number	VARCHAR(14), Membership_num VARCHAR(20), CampaignCode	VARCHAR(10)		); ")
	if err != nil {
		message = "Error creating M3_Checks - 5 : "
	    fmt.Println("err x3: ", err)
        //return 
    } else {
    	fmt.Println("M3_Checks successfully created")
    }

    //
    // Load all the daily files into SQL Server
    //

    fl := fileName
    loadFiles := loadFiles(fl, db)
    fmt.Println("After loadFiles: ",loadFiles)

    //
    // Load M3_Checks (Data from RBE)
    //
    loadFiles = loadM3Checks("M3_Checks.txt", db)
    fmt.Println("After loadM3Checks: ",loadFiles)
    if loadFiles != "Success" {
		message = loadFiles
    	return
    }

	//
	//	Daily comparison        --> maybe create a stored proc to do check
	//							--> Keep data in SQL tables
    fmt.Println("About to Compare data ")
    compareSuccess := compareData(db)
    if compareSuccess == "failure" {
    	fmt.Println("Comparrison not done: ", compareSuccess)
    	return
    }
    fmt.Println("Comparrison done: ", compareSuccess)

    //
    // This function will create the following reports
    // 		- Daily report:  To be found on P3JHBFS01..Reports     
    //		- Daily discrepancies: sent to support team and found on P3ISM3VM/P3ISM3DEV/P3ISM3FOVM
    checkReport := createReports(db)
    fmt.Println("Report done: ", checkReport)

	return
}	
 

// -- Working
//******************************************************************************************
//
//  Load all files from M3 into SQL
//
//******************************************************************************************
func createReports(db *sql.DB) (ret string){
	
	t := time.Now()
	fmt.Println("Run date : ", t)

	tDate := t.Format("20060102")

	rows, err := db.Query("SELECT CampaignID, TotalInRBE, TotalInM3, InRBENotM3, InM3NotRBE from [dbo].[M3DailyBalances] WHERE date = ? ORDER BY CampaignID", tDate)
	if err != nil {
		fmt.Println("Query error: ", err)
		//log.Fatal(err)
	}
	defer rows.Close()

	csvfile, ferr := os.Create("E:\\M3_P3_Admin\\DailyRBEvsM3Report.csv")
	if ferr != nil {
        fmt.Println("Error:", ferr)
        return "create file error"
    }
    defer csvfile.Close()


	prn := fmt.Sprintln("Campaign ID 	Total in RBE	Total in M3    In RBE not M3 	In M3 not RBE")	
	_, werr := io.WriteString(csvfile, prn)
	if werr != nil {
		fmt.Println("Error writing to csvfile")
	}
	prn = fmt.Sprintln("***********    ************ 	***********    ************* 	************* ")
	_, werr = io.WriteString(csvfile, prn)
	if werr != nil {
		fmt.Println("Error writing to csvfile")
	}
	
	prn = fmt.Sprintln(" ")		
	_, werr = io.WriteString(csvfile, prn)
	if werr != nil {
		fmt.Println("Error writing to csvfile")
	}

	fmt.Println("Campaign ID 	Total in RBE	Total in M3    In RBE not M3 	In M3 not RBE")	
	fmt.Println("***********    ************ 	***********    ************* 	************* ")
	fmt.Println(" ")		

	for rows.Next() {
		
		var (
		//date string
		CampaignID string
		TotalInRBE int
		TotalInM3 int
		InRBENotM3 int
		InM3NotRBE int
		)

		err := rows.Scan(&CampaignID, &TotalInRBE, &TotalInM3, &InRBENotM3, &InM3NotRBE)
		if err != nil {
			fmt.Println("rows.Next error: ", err)
			//log.Fatal(err)
		}
		prn = fmt.Sprintf("%s\t\t%d\t\t%d\t\t%d\t\t%d \n", CampaignID, TotalInRBE, TotalInM3, InRBENotM3, InM3NotRBE)

		_, werr = io.WriteString(csvfile, prn)
		if werr != nil {
			fmt.Println("Error writing to csvfile")
		}
		
		prn = fmt.Sprintln("-----------------------------------------------------------------------------*")

		_, werr = io.WriteString(csvfile, prn)
		if werr != nil {
			fmt.Println("Error writing to csvfile")
		}

		fmt.Printf("%s\t\t%d\t\t%d\t\t%d\t\t%d \n", CampaignID, TotalInRBE, TotalInM3, InRBENotM3, InM3NotRBE)
		fmt.Println("-----------------------------------------------------------------------------*")
	}

	fmt.Println("c")

	err = rows.Err()
	if err != nil {
		fmt.Println("Huh?: ", err)
		//log.Fatal(err)
	}
	
	return
}

//  -- Working
//******************************************************************************************
//
//  M3_daily_balances.sql is a SQL script written to do all the heavy work within SQL itself
//
//******************************************************************************************
func compareData(db *sql.DB) (ret string){

 	parts := []string{"-S", "localhost", "-i", "E:\\M3_P3_Admin\\M3_daily_balances.sql"}
    fmt.Println("Hold Thumbs!!!!!")

    rets, err := exec.Command("SQLCMD", parts...).Output()

    fmt.Println("...and err : ", err)
    //fmt.Println("...and rets: ", rets)
    if err != nil {
	   	fmt.Println("**************************************")
	   	fmt.Println("result: ",rets)
        fmt.Println("err: ", err)
	   	fmt.Println("**************************************")
	   	ret = "failure"
        return 
        }    
    ret = "success"
    return 
 }


// --Working
//******************************************************************************************
//
//	Loads the full daily file from RBE
//
//******************************************************************************************
//C:\Users\briana\Documents\SQL Server Management Studio\OPenRowSet For M3Checks.sql


func loadM3Checks(fileName string, db *sql.DB) (ret string){

 	parts := []string{"-S", "localhost", "-i", "E:\\M3_P3_Admin\\OPenRowSet For M3Checks.sql"}
    fmt.Println("Hold Thumbs!!!!!")

 	rets, err := exec.Command("SQLCMD", parts...).Output()

	if err != nil {
	  	ret = "Oooh - Problems loading data into M3_Checks"
	    fmt.Println("result: ",rets)
        fmt.Println("err: ", err)
        return 
    }    
    
    ret = "Success"
    return
}


// --Working
//******************************************************************************************
//
//	Loads all the daily files from M3 in SQL
//
//******************************************************************************************

func loadFiles(fileName []string, db *sql.DB) (ret string){

	t := time.Now()
	fmt.Println("Run date : ", t)

	fmt.Println("Run day : ", t.Weekday())

	//    
	//  Load all files into 1 SQL table
	//
	for _, fName := range fileName {
		
		if fName == "" {
			break
		}
		// on a Monday, use 'AR0045 weekend'
		f := fName
		if fName == "AR0045" {
			if t.Weekday() == 1 {
				fmt.Println("Catering for Monday!")
				continue
			}
		}

		if fName == "AR0045 weekend" {
			f = "AR0045"
			if t.Weekday() != 1 {
				fmt.Println("Catering for rest of week!")
				continue
			}
			fmt.Println("Changed AR0045_Weekend to AR0045")
		}
		
		//  remove old data before loading current data
		_, err := db.Exec("IF OBJECT_ID('M3.dbo.M3_daily') IS NOT NULL DROP TABLE M3.dbo.M3_daily") 
		if err != nil {
			ret = "Error dropping M3_Daily - 3 : "
	    	fmt.Println("err: ", err)
        	//return 
    	} else {
    		fmt.Println("M3_daily successfully found and dropped")
    	}

    	_, err = db.Exec("CREATE TABLE [M3].[dbo].[M3_daily] ([SA_ID_Number] VARCHAR(13), [Membership_num] VARCHAR(14), [email] VARCHAR(150), [cell] varchar(20))")

		fmt.Println("Processing ", fName)
	    
	    // do BULK INSERT with each filename
	    sqlInsert := "BULK INSERT [M3].[dbo].[M3_daily] FROM 'E:\\SDLFTP\\SDLFTP\\Daily\\" + fName + "' WITH (  FIRSTROW = 2, FIELDTERMINATOR = '|',  ROWTERMINATOR = '\n',  ERRORFILE = 'E:\\M3 Export\\M3_Errors',  TABLOCK )"
    	
    	fmt.Println("SQL Insert command: ", fName)	

 		result, err := db.Exec(sqlInsert) 

	    if err != nil {
	        fmt.Println("result: ",result)
            fmt.Println("err: ", err)
            return 
        }    

        sqlCopyTo := "IF OBJECT_ID('M3.dbo.M3_AllCampaigns') IS NULL SELECT [SA_ID_Number], [Membership_num], [email], [cell], '" + f + "' AS campaign_ID INTO [M3].[dbo].[M3_AllCampaigns]  FROM [M3].[dbo].[M3_daily] ELSE INSERT INTO [M3].[dbo].[M3_AllCampaigns] SELECT [SA_ID_Number], [Membership_num], [email], [cell], '" + f + "' AS fileName FROM [M3].[dbo].[M3_daily]"
    	
    	fmt.Println("SQL copy command: ", fName)	

 		resultCopy, err := db.Exec(sqlCopyTo) 

	    if err != nil {
	        fmt.Println("result: ",resultCopy)
            fmt.Println("err: ", err)
            return 
        }    
	}	
	ret = "Success"
	return
}


// --Working
//******************************************************************************************
//
//  Create a list of all files to process found in 'Daily' directory
//
//******************************************************************************************

func getFileList() (fileInfo []string) {

	dirname := "E:\\SDLFTP\\SDLFTP\\Daily"
	fileIn, error := ioutil.ReadDir(dirname) 

	check(error)

	l := len(fileIn)
	fmt.Println("length of L: ", l)

	newSlice := make([]string, (l+len(fileIn)))

	fmt.Println("length of newSlice: ", len(newSlice))

	copy(newSlice, fileInfo)
	fileInfo =  newSlice
	for x, f := range fileIn {
          fileInfo[x] = f.Name()
    }

    return
}


//  -- Need to prep proper config file
//  -- Create Struct for config file
//  -- pass back data in 'load', but as a struct
// 
//******************************************************************************************
//
//  Load config file
//
//******************************************************************************************

/* func loadConfig() (load string) {
	configData, err :=  ioutil.ReadFile("E:\\M3_P3_Admin\\DailyCheck.config")
	check(err)
	fmt.Println(string(configData))
	load = "Yaya"
	return 
} */


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