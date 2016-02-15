/******************************************************************************/
/* removes all files which are empty, skips weekends                          */
/******************************************************************************/
package main

import (
  "fmt"
  "os"
  "time"
  "io/ioutil"
)

func main() {

  	t := time.Now()
  	fmt.Println("Run date : ", t)

  	fmt.Println("Run day : ", t.Weekday())

    switch t.Weekday() {
    case 0 : fmt.Println("Sunday")
    case 7 : fmt.Println("Saturday")
    default: fmt.Println("default")
      	 dirname := "E:\\SDLFTP\\SDLFTP\\Daily"
  	     fileIn, error := ioutil.ReadDir(dirname)

         fileFlag := 0
         name := "AR0000"
         if error != nil {
           fmt.Println(error)
           } else {
              i := 0
  	          for k, v := range fileIn {
                fileFlag = cleanFile(v.Name())
                name = v.Name()

                if fileFlag == 1 {
                  break
                }

                if fileFlag == 2 {
                fileIn[i] = fileIn[k]
                error = os.Remove("E:\\SDLFTP\\SDLFTP\\Daily\\" + name)

                if error != nil {
                  fmt.Println("failure to delete ", fileIn[i])
                } else {
                    fmt.Println("deleted ", name)
                }
                continue
              }

              fileIn[i] = fileIn[k]
              i++
            }
        }
    }
}


/******************************************************************************/
// remove all entries without any data for the day.
/******************************************************************************/
func cleanFile(fName string) (fileError int) {
  xfName := string(fName)
  fileContent, err := ioutil.ReadFile("E:\\SDLFTP\\SDLFTP\\Daily\\" + xfName)

  if err != nil {
    fmt.Println("problem: ", err)
    return 1
  } else {
    // length of 66 means it only has headings and no content
    if len(fileContent) == 66 {
      // remove from list
      fmt.Println("xfName: ", xfName)
      return 2
    }
    return 0
  }
}
