package SBRules_test

import (

//	"standardBankTest"
	"standardBankTest/SBRules"
	"testing"
)


func TestSBRules(t *testing.T) {

	id := []string{
//normal	
			"7002165206086",
//too short
			"730417516808",
// month = 00
			"7000065206086",
// day = 00
			"7001005206086",
// month > 12
			"7022065206086",
// day > 31
			"7002445206086",
// non numeric
			"7002165AV6086",
// non SA ID
			"7002165206186",
	}


message := []string{ 
			"ok",
			"ID incorrect length!",
			"bad Date of Birth!",
			"bad Date of Birth!",
			"bad Date of Birth!",
			"bad Date of Birth!",
			"Non Numeric ID!",
			"Non SA Citizen!",
//			"bad Check Digit!",

//			"bad Gender!",   
	}
	
	for i := 0; i < len(id); i++ {
		expected 	:= message[i]
		actual 		:= SBRules.ValidateID(id[i])
		if actual != expected {
			t.Errorf("Test failed, expected : '%s', got '%s'", expected, actual)
		}
	}

}