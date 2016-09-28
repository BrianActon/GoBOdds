package main //standardBankTest

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"standardBankTest/SBRules"
	//	"io/ioutil"
	//	"net/http"
)

func main() {


	fileName := "SAID_Check.txt"

	data, err := loadFile(fileName)
	if err == nil {
		err = processFile(data)
		if err == nil {
			fmt.Println("success")
		} else {
			fmt.Println("Process file failure", err)
		}
	} else {
		fmt.Println("Load file failure", err)
	}

}

//
// load file with IDs, create channels and use goroutine to process each ID individually
//
func processFile(fileName []string) error {

	fmt.Println(len(fileName))

	/* create buffered channels*/
	validIDChannel 	:= make(chan string, 1000)
	badIDChannel 	:= make(chan string, 1000)

	defer close(validIDChannel)
	defer close(badIDChannel)

	for _, v := range fileName {
		go SBRules.ProcessID(v, validIDChannel, badIDChannel)
	}

	a := 0
	b := 0

	/* slices to contain output from channels */
	var validIDSlice []string
	var badIDslice []string

	/* */
	for i := 0; i < len(fileName); i++ {

		select {

		case goodID := <-validIDChannel:
			/* add to validIDSlice*/
			a++
			validIDSlice = append(validIDSlice, goodID)

		case badID 	:= <-badIDChannel:
			/* add to InvalidIDSlice*/
			b++
			badIDslice = append(badIDslice, badID)
		}

	}

	/* pidbSlice to contain breakdown in 4 arrays for CSV file*/
	stDim := len(validIDSlice)

	pidbSliceID 		:= make([]string, stDim)
	pidbSliceDOB 		:= make([]string, stDim)
	pidbSliceGender 	:= make([]string, stDim)
	pidbSliceCitizen 	:= make([]string, stDim)

	for i := 0; i < stDim; i++ {

		pidbSliceID[i], pidbSliceDOB[i], pidbSliceGender[i], pidbSliceCitizen[i] = breakDown(validIDSlice[i])

	}

	pidbSlice := make([][]string, stDim)

	for i := 0; i < stDim; i++ {

		pidbSlice[i] = make([]string, 4)

		pidbSlice[i][0] = pidbSliceID[i]
		pidbSlice[i][1] = pidbSliceDOB[i]
		pidbSlice[i][2] = pidbSliceGender[i]
		pidbSlice[i][3] = pidbSliceCitizen[i]

	}

	if len(pidbSlice) > 0 {
		/* write pidb to csv */
		err := writePIDB(pidbSlice)
		fmt.Println(err)
	}

	if len(badIDslice) > 0 {
		/* write badIDslice to text file */
		err := writeBadIDs(badIDslice)
		fmt.Println(err)
	}

	fmt.Println("a = ", a)
	fmt.Println("b = ", b)

	return nil
}

//
// Writes out all the good ID's to .csv file
//
func writePIDB(pidb [][]string) error {
	fmt.Println("writePIDB")

	f, err := os.Create("pidb.csv")
	if err != nil {
		fmt.Println("Cannot create file ", err)
	}
	defer f.Close()

	wr := csv.NewWriter(f)

	wr.WriteAll(pidb) // calls Flush internally

	return err
}

//
// Writes out all the bad ID's normal text file
//
func writeBadIDs(badID []string) error {

	/*  output cluttered */

	f, err := os.Create("BadIDFile.csv")
	if err != nil {
		fmt.Println("Cannot create BadID file ", err)
		return err
	}
	defer f.Close()

	for i := 0; i < len(badID); i++ {
		s := (badID[i] + "\n")
		_, err = io.WriteString(f, s)

		if err != nil {
			fmt.Println("Cannot write BadID file ", err)
		}

	}

	return nil
}

//
// load file with IDs, create channels and use goroutine to process each ID individually
//
func breakDown(id string) (string, string, string, string) {

	var idbytes = []int{}
	ida := []byte(id)
	for _, i := range ida {
		idbytes = append(idbytes, int(i))
	}

	gender := "Male"
	if (idbytes[6] - 48) < 5 {
		gender = "Female"
	}

	for i := 0; i < len(ida); i++ {
		idbytes[i] -= 48
	}
	x := idbytes[:6]
	//  fmt.Println(x)
	dob := changeDate(x)

	return id, dob, gender, "SA"
}

//
// Quick date format
//
func changeDate(id []int) string {

	//    fmt.Println("id ", id)

	arrCen 	:= 20
	arrYr 	:= (byte(id[0]) * 10) + byte(id[1])
	arrMon 	:= (byte(id[2]) * 10) + byte(id[3])
	arrDay 	:= (byte(id[4]) * 10) + byte(id[5])

	/* sort out year */
	/* will be problematic for those pesky healthy centenarian */
	if arrYr > 20 {
		arrCen = 19
	}

	month := "none"
	switch arrMon {
	case 1:
		month = "Jan"
	case 2:
		month = "Feb"
	case 3:
		month = "Mar"
	case 4:
		month = "Apr"
	case 5:
		month = "May"
	case 6:
		month = "Jun"
	case 7:
		month = "Jul"
	case 8:
		month = "Aug"
	case 9:
		month = "Sep"
	case 10:
		month = "Oct"
	case 11:
		month = "Nov"
	case 12:
		month = "Dec"
	default:
		month = "error"
	}

	x := fmt.Sprintf("%d, %s, %d%d", arrDay, month, arrCen, arrYr)
	return x
}

//
// load file with IDs, create channels and use goroutine to process each ID individually
//
func loadFile(fileName string) ([]string, error) {
	file, err 	:= os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner 	:= bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
