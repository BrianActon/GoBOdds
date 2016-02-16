/******************************************************************************/
/* Checks to see all campaigns have run and reports discrepancies.            */
/* No need to run on weekends                                                 */
/******************************************************************************/
package main

import(

	"os"
	"os/exec"
  "log"
  //"net/smtp"
	"time"
	"strings"
	"io/ioutil"
	"fmt"
  "encoding/csv"
  //"bytes"
	_ "github.com/denisenkom/go-mssqldb"
 // "database/sql"
	)

func main() {

    fmt.Println("Started!")

		t := time.Now()
		fmt.Println("Run date : ", t)

		fmt.Println("Run day : ", t.Weekday())

		switch t.Weekday() {
		case 0 : fmt.Println("Sunday")
		case 7 : fmt.Println("Saturday")
		default:
			fmt.Println("default")
      // get Daily list
      DailyFiles := getDailyFileList()

      // Daily run completed list
      RunFiles := getDaily_Run_CompletedFileList()

      // compare lists
      missing := compareLists(DailyFiles, RunFiles)

		  // notify m3support@p-cubed.co.za of any errors
      if missing[0] == "" {
        fmt.Println("nothing missing")
      } else {
  		  notified := notifyM3Support(missing)
        if notified == "no" {
          fmt.Println("buggered from sqlcmd")
        }
      }

      // move all Daily Run Cpmpleted files to 'Done' folder
      mover := moveDailyRunCompletedFiles(RunFiles)
      if mover != "done" {
        fmt.Println(mover)
      }

      fmt.Println("Prog Finished!")

		  os.Exit(0)
    }

}

// --Works
//******************************************************************************************
//
//  Move files to /Done and delete from main folder
//
//******************************************************************************************
func moveDailyRunCompletedFiles(runFiles []string) (msg string){
    fmt.Println("moveDailyRunCompletedFiles: ")

    cmd := exec.Command("E:\\ILOADERPROCESSFILES\\moveDailyRunFiles.bat")
    fmt.Println("Command is : ", cmd)
	  err := cmd.Run()
	  if err != nil {
		  log.Fatal(err)
	  }
    return "done"
}


// -- works
//******************************************************************************************
//
//  Notify M3Support.P-cubed.co.za via smtp (email)
//
//******************************************************************************************
func notifyM3Support(missing []string) (string) {

    fmt.Println("notifyM3Support: ", missing)

    // open output file
    csvfile, err := os.Create("E:\\M3_P3_Admin\\DailyRunMisMatch.csv")
    if err != nil {
        fmt.Println("Error:", err)
        return "create"
    }
    defer csvfile.Close()

    writer := csv.NewWriter(csvfile)
    err = writer.Write(missing)
    if err != nil {
      fmt.Println("Error:", err)
      return "NewWrite"
    }

    writer.Flush()

    return "done"
}

// -- works
//******************************************************************************************
//
//  Compare the 2 lists and returning a slice with all campaign files missing
//
//******************************************************************************************
func compareLists(dailyFiles []string, runFiles []string) ([]string){

    fmt.Println("compareLists: ")
    found := make([]string, (len(dailyFiles)))
    missing := make([]string, (len(dailyFiles)))

    i := 0
    j := 0
    for _, dfName := range dailyFiles {
      if dfName == "AR0045 weekend" {
        continue
      }
      f := i
      for _, rfName := range runFiles {
        if strings.HasPrefix(rfName, dfName) {
          found[i] = dfName
          i++
          break
        }
      }
      // if not equal then match found
      if f == i {
        missing[j] = dfName
        j++
      }
    }

		return missing
}


// -- Works
//******************************************************************************************
//
//  Loads all Daily files from M3Checks
//
//******************************************************************************************
func getDailyFileList() (fileInfo []string) {

  fmt.Println("getDailyFileList: ")
	dirname := "E:\\SDLFTP\\SDLFTP\\Daily"
	fileIn, error := ioutil.ReadDir(dirname)

	check(error)

	newSlice := make([]string, (len(fileIn)))

	copy(newSlice, fileInfo)
	fileInfo =  newSlice
	for x, f := range fileIn {
          fileInfo[x] = f.Name()
    }

    return
}

// -- Working
//******************************************************************************************
//
//   Loads all daily_run_completed files for all campaigns that have run
//
//******************************************************************************************

func getDaily_Run_CompletedFileList() (fileInfo []string) {

  fmt.Println("getDaily_Run_CompletedFileList: ")
	dirname := "E:\\SDLFTP\\SDLFTP\\daily_run_completed"
	fileIn, error := ioutil.ReadDir(dirname)

	check(error)

	newSlice := make([]string, (len(fileIn)))

	copy(newSlice, fileInfo)
	fileInfo =  newSlice
	for x, f := range fileIn {
          fileInfo[x] = f.Name()
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
