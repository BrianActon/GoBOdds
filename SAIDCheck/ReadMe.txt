Rules for invalid SA ID:

//  Format for SA ID  :  {YYMMDD}{G}{SSS}{C}{A}{Z}
//  Check all numeric?
//  Check if it is 13 digits.
//  Check if first 6 a valid date?
//  No need to check position 7 : (0 - 4)female  (5 - 9)male
//  position 11 = 0  if South African and 1 if not
//  position 13 is control digit, formulae available http://geekswithblogs.net/willemf/archive/2005/10/30/58561.aspx


Usage:

Create a file called SAID_check.txt
fill it 1 ID per line
compile and run with SAID_check.txt in same folder