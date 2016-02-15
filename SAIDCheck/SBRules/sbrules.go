package SBRules

import (
	"strconv"
)

//
//  test SA ID validation  :  {YYMMDD}{G}{SSS}{C}{A}{Z}
//  is it all numeric?
//	is it 13 digits?
//  first 6 a valid date?
//  position 7  (0 - 4)female  (5 - 9)male
//  position 11 = 0  if South African and 1 if not
//  position 13 is control digit
//
func ProcessID(id string, validIDChannel chan<- string, badIDChannel chan<- string) (out string){

	err := ValidateID(id)
	if err != "ok" {
		badIDChannel    <- id
		out = err
	} else {
		validIDChannel	<- id
		out = "SA"
	}

	return 
}

//
//  first failure exits 
//
func ValidateID(id string) (out string){
	
	err := ValidateNumeric(id)
	if err != "ok" {
		return "Non Numeric ID!"
	}
	
	var idbytes = []int{}
	ida :=  []byte(id)
	for _, i := range ida {
        idbytes = append(idbytes, int(i))
    }
	
	err = ValidateIDLength(idbytes)
	if err != "ok" {
		return "ID incorrect length!"
	}

// pointless test
	err = ValidateGender(idbytes)
	if err != "ok" {
		return "bad Gender!"
	}
		
	err = ValidateDate(idbytes)
	if err != "ok" {
		return "bad Date of Birth!"
	}
	
	err = ValidateSACitizen(idbytes)
	if err != "ok" {
		return "Non SA Citizen!"
	}

	err = testCheckDigit(idbytes)
	if err != "ok" {
		return "bad Check Digit!"
	}

	return "ok"
}


//
// Test if ID is numeric by checking if the string can be converted.
//
func ValidateNumeric(id string) (string) {
	
	_, err := strconv.Atoi(id)
	
	if err == nil {
 //  		fmt.Println("Numeric test passes")
	} else {
		return "non numeric"
	}
	
	return "ok"
}

//
// SA ID is 13 characters long
//
func ValidateIDLength(id []int) (string) {
	if len(id) != 13 {
		return "Bad length"
	} else {
		return "ok"
	}
}


//
// position 7 = 0 - 4 if Female and 5 - 9 for Male
//
func ValidateGender(id []int) (string) {
//	fmt.Println("Gender digit: ", (id[6] - 48))
	return "ok"
}


//
// yymmdd
// yy  - what if someone lives to be 100 or more?
// mm  - check mm not = 00 or > 12 
// dd  - bit more fun: 
//		cant be = 00
//		if feb - test for leap year then cant be >29 else not > 28
//		Jan, Mar, May, Jul, Aug, Oct and Dec then not > 31
//		rest months, dd not > 30
//
func ValidateDate(id []int) (string) {

	for i := 0; i < len(id); i++ {
        id[i] -= 48
    }

    arrYr  := (byte(id[0]) * 10) + byte(id[1])
    arrMon := (byte(id[2]) * 10) + byte(id[3])	
    arrDay := (byte(id[4]) * 10) + byte(id[5])
	
	leapYear := arrYr % 4
	
    switch true {
	/*test valid month range*/	
    case (arrMon == 0) || (arrMon > 12):
        return "Month out of range"
    /* test valid max day range*/    
    case ((arrDay == 0) || (arrDay > 31)):
        return "Day out of range"
    /* Jan, Mar, May, Jul, Aug catered for above*/
    /* Apr, Jun, Sept, Nov*/    
    case ((arrMon == 4) || (arrMon == 6) || (arrMon == 9) || (arrMon == 11)) && (arrDay > 30):
        return "too many days in month"
    /* feb */    
    case (leapYear == 0) && (arrMon == 2) && (arrDay > 29):
        return "too many days in month"
    case (leapYear > 0) && (arrMon == 2) && (arrDay > 28):
        return "too many days in month"
    }

	return "ok"
}


//
// position 11 = 0 if South African and 1 if not
//
func ValidateSACitizen(id []int) (string) {
	if id[10] == 0 {
		return "ok"
	} else {
		return "Non-SA ID"
	}
}


//
// formulae taken from  "http://geekswithblogs.net/willemf/archive/2005/10/30/58561.aspx"
//
func testCheckDigit(id []int) (string) {

  	a := id[0] + id[2] + id[4] + id[6] + id[8] + id[10]

  	b := (id[1] * 100000) + (id[3] * 10000) + (id[5] * 1000) + (id[7] * 100) + (id[9] * 10) + (id[11])

  	b *= 2
  	addEvenDigits := 0

  	for i := 0; i < 6; i++ {
  		addEvenDigits += (b % 10)
  		b /= 10
  	}

  	c := a + addEvenDigits

	d := 10 - (c % 10)
	if d == 10 {
		d = 0
	}

  	if d == id[12] {
  		return "ok"
  	} else {
  		return "Check digit failed"
  	}	
} 