package main

import(
	"fmt"
	"io/ioutil"
)


func main() {
	

	emailCollectionALL := make([]string, 1)

    dirname := "E:\\OptionsHTML\\"
  	folders, err := ioutil.ReadDir(dirname)

  	if err != nil {
     	fmt.Println("folder fail - bugger: ", err )
	} else {
	   	for _, v := range folders {
	   		xfName 	:= v.Name()
	   		fPathName 	:= fmt.Sprint(dirname + xfName + "\\")
	   	//	log.Println(fPathName)
	  	// 	fmt.Println(fPathName)

  			files, err := ioutil.ReadDir(fPathName)

  			if err != nil {
     			fmt.Println("file fail - bugger: ", err )
			} else {
	   			for _, v := range files {

	   				fileName := v.Name()
	   				fullPath := fmt.Sprint(fPathName + fileName)
	  	
	  	   			e, ferr := loadFileIntoSlice(fullPath)

	  	   			if ferr != nil {
	  	   				fmt.Println("Load error: ", ferr)
	  	   			}

	  	   			for _, v := range e {
	  	   				emailCollectionALL = append(emailCollectionALL, string(v))
	  				}	
	  			}   
	  		}	

		}
	}

	for k, v := range emailCollectionALL {
		fmt.Println(k, string(v))
	}
}


func loadFileIntoSlice(fPathName string) ([]string  , error) {

	fmt.Println("fPathName ", fPathName)

	fileContent, err := ioutil.ReadFile(fPathName)

	emailCOllection := make([]string, 1)
	storeEmail := make([]byte, 1)

	if err != nil {
		fmt.Println(" --> nooooo ")
		return emailCOllection, err
	} else {

		compareS := []byte{'m', 'a', 'i', 'l', 't', 'o', ':'}
		fmt.Println("Length ", len(compareS))
		fmt.Println("compareS ", string(compareS))
		fmt.Println("Length fileContent ", len(fileContent))
	
		for k, _ := range fileContent {
	
			if  k > (len(fileContent ) - 10) {
				break
			}

			// not working with &&. Need to investiagte why.

			if  fileContent[k] 	 == compareS[0]  	{ 

				if  fileContent[k+1] == compareS[1]  	{ 

					if  fileContent[k+2] == compareS[2]  	{ 

						if  fileContent[k+3] == compareS[3]  	{ 

							if  fileContent[k+4] == compareS[4]  	{ 

								if  fileContent[k+5] == compareS[5]  	{ 

									if  fileContent[k+6] == compareS[6] 	{

										storeEmail = nil

						//				fmt.Println(" helloo  :-> ", string(fileContent[k]), string(v), k)

										for j := k + 7; j < len(fileContent) ; j++ {
											if j > (k+50) {
												break
											}
											if  fileContent[j] == '"' {
												break
											}

											storeEmail = append(storeEmail, fileContent[j])
										}

										emailCOllection = append(emailCOllection, string(storeEmail))
									}
								} 
							} 
						} 
					} 
				} 
			} 
		}
	}

//	for k, v := range emailCOllection {
//		fmt.Println(v)
//	}

	fmt.Println("Thanks")
	return emailCOllection, err


}

