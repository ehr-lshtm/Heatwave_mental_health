/*=========================================================================
DO FILE NAME:			4_psychiatric_heat-japan-01-prep_sccs-program

AUTHOR:					Angel Wong	
						
VERSION:				v1
					
DATABASE:				Japanese claims data
	
DESCRIPTION OF FILE:	
1.divide the observation period into drug exposed and drug non-exposed periods
2.Run SCCS using heat as an exposure in people with mental disorder
											
DO FILES NEEDED:	NA

ADO FILES NEEDED: 	NA

MORE INFORMATION:	
****************************************************************************************************
*** Prepare data in the right format for SCCS ******************************************************
****************************************************************************************************/

/* 
We will generate a dataset with one row per interval and the following variables:

patid 		- individual patient identifier
agegp 		- age category (5 year band)
season	 	- season category
interstart	- interval start date
interend	- interval end date (last day included in interval + 1)
interval 	- interval duration
loginterval - natural log of interval duration
intertype* 	- categorical variable for each exposure type (named using labels of prodtype)
outcome_ind  - binary indicator for outcome 
*/

* The values of exposure (i.e. intertype) should be
* 0 - baseline period
* 1 - 5 day pre-exposure period
* 2 - heatwave period
* 3 - 5 days on/ after heatwave


*********************************************************************
capture log close
* open log file - no need as fast tool will create log files
log using "${pathLogs}/4_psychiatric_heat-japan-01-prep_sccs-program", text replace
*********************************************************************

/*****************************************************************
* 1. Create drug exposure intervals
*****************************************************************/	
use "$datafile/psychiatric_clean_drug_all", clear

rename rxst_antipsy exp_st
rename rxen_antipsy exp_en

keep patid dob exp_st exp_en st_st st_en city_code

sort patid exp_st exp_en
format st_st %td
bysort patid (exp_st): gen nextfirstrx = exp_st[_n+1] 
bysort patid (exp_st): gen prevepiend = exp_en[_n-1] if _n != 1
format nextfirstrx prevepiend %td

* Create interval for pre-exposure period, for each risk period, and for end of risk periods
expand 2
bysort patid exp_st: gen intertype = _n - 1
*label values intertype intertype_lbl

* Create interval dates for current exposure period
gen interstart = exp_st if intertype == 1
gen interend = exp_en if intertype == 1
format interstart interend %td

* Create interval dates for non-risk period
bysort patid: gen lastpostinterval = interend[_n-1]
bysort patid: gen nextpreinterval = interstart[_n+1]
format lastpostinterval nextpreinterval %td
replace interstart = lastpostinterval + 1 if intertype == 0 & lastpostinterval!=.
replace interend = nextpreinterval - 1 if intertype == 0 & lastpostinterval!=.

* Create first non-risk period from study start
preserve
keep if intertype == 0 & lastpostinterval==.
tempfile lastnonriskperiod
save `lastnonriskperiod'
unique patid
restore

replace interstart = st_st if intertype == 0 & lastpostinterval==.
replace interend = nextpreinterval - 1 if intertype == 0 & lastpostinterval==.
drop if interstart> interend & intertype == 0 

append using `lastnonriskperiod'

* Create last non-risk period until study end
sort patid interstart
drop lastpostinterval nextpreinterval
replace interstart = interend[_n-1] + 1 if intertype == 0 & interstart ==.
replace interend = st_en if intertype == 0 & interend ==.

* Drop intervals that are not valid
drop if interstart > interend
drop if interend < st_st
replace interend = st_en if interend > st_en
replace interstart = st_st if interstart <st_st
drop if interstart > interend
drop if interend < st_st

assert interstart <= st_en 
assert interend >= st_st

* Edit the last non-risk window for the end date
replace interend = st_en if interend == . & intertype == 0

* Edit the first start date of episode for each person as st_st and the last end date of episode as st_en
bysort patid: gen indiv_order = _n
bysort patid: gen total_episode = _N
replace interstart = st_st if indiv_order == 1
bysort patid: replace interend = st_en if _n == _N

* Validity checks
assert interstart <= interend
bysort patid exp_st interstart: assert _n == 1
assert interstart == floor(interstart)

sort patid interstart interend
bysort patid: gen nextpost_st = interstart[_n+1]
format nextpost_st %td

assert interend + 1 == nextpost_st if nextpost_st != .

sort patid interstart interend
bysort patid: gen lastpost_en = interend[_n-1]
format lastpost_en %td

assert lastpost_en + 1 == interstart if lastpost_en != .

* Save relevant data
keep patid intertype st_st st_en interstart interend dob exp_st exp_en city_code
order patid dob st_st st_en exp_st exp_en intertype interstart interend city_code
rename intertype drugexposed
rename exp_st rxst_antipsy
rename exp_en rxen_antipsy
save temp_drugexposure_intervals.dta, replace

/*****************************************************************
* 2. merge heatwave data
*****************************************************************/	
*key city category code to city code
merge m:1 city_code using "$datafile/city_codebook_v2", keep(master match) keepusing(city_cat_code) nogen

tab city_cat_code,m

forval num = 1/6 {
	preserve
	keep if city_cat_code == `num'
	save "psychiatric_clean_drug_all_`num'", replace
	restore
}

*key the heatwave data to antipsychotics data stratified by 6 cities
forval num = 1/6 {

use "psychiatric_clean_drug_all_`num'", clear

*set up risk period (heatwave)
gen obs=1
joinby obs using "$datafile/MEANT_heatwave_city`num'_date"
gen exposure = 1 if interstart<=hw_start & hw_start<=interend
replace exposure = 1 if interstart<=hw_end & hw_end<=interend

gen exp_st = max(hw_start, interstart)
gen exp_en = min(hw_end, interend)
format exp_st exp_en %td

replace exp_st = . if exposure == .
replace exp_en = . if exposure == .

duplicates drop patid drugexposed interstart exp_st, force
drop if exp_st > exp_en

drop obs exposure

save "psychiatric_clean_drug_all_heatwave_`num'", replace

}

*combine all dataset into one
use "psychiatric_clean_drug_all_heatwave_1", clear
forval num = 2/6 {
append using "psychiatric_clean_drug_all_heatwave_`num'"
}
save "$datafile/psychiatric_clean_drug_all_heatwave", replace

sort patid interstart interend exp_st exp_en
duplicates drop patid interstart, force
drop hw_start hw_end exp_st exp_en
bysort patid: gen num_interval = _N
su num_interval
return list
local max=r(max)

bysort patid: gen seq = _n

merge 1:m patid interstart using "$datafile/psychiatric_clean_drug_all_heatwave", keepusing(exp_st exp_en) nogen

save "$datafile/psychiatric_drug_heatwave_cat", replace
	
/*****************************************************************
* 3. Create heat exposure intervals for each drug exposed interval
*****************************************************************/	
forval num_interval = 1/`max' {
use "$datafile/psychiatric_drug_heatwave_cat", clear
keep if seq == `num_interval'
	
rename st_st st_st_old
rename st_en st_en_old

rename interstart interstart_old
rename interend interend_old

bysort patid: egen st_st = min(interstart)
bysort patid: egen st_en = max(interend)

format st_st st_en %td

sort patid exp_st exp_en
bysort patid (exp_st): gen nextfirstrx = exp_st[_n+1] 
bysort patid (exp_st): gen prevepiend = exp_en[_n-1] if _n != 1
format nextfirstrx prevepiend %td

* Create interval for pre-exposure period, for each risk period, and for end of risk periods
expand (2 + 2)
bysort patid exp_st: gen intertype = _n - 1
*label values intertype intertype_lbl

* Create interval dates for pre-exposure period
gen interstart = exp_st - 5 if intertype == 1 //change 5 as length of pre-exposure period
gen interend = exp_st - 1 if intertype == 1

format interstart interend %td

* Create interval dates for current exposure period
replace interstart = exp_st if intertype == 2
replace interend = exp_en if intertype == 2

* Create interval date for post exposure period
replace interstart = exp_en + 1 if intertype == 3
replace interend = exp_en + 5 if intertype == 3 //change 5 as length of post-exposure period

* Create interval dates for non-risk period
bysort patid: gen lastpostinterval = interend[_n-1]
bysort patid: gen nextpreinterval = interstart[_n+1]
format lastpostinterval nextpreinterval %td
replace interstart = lastpostinterval + 1 if intertype == 0 & lastpostinterval!=.
replace interend = nextpreinterval - 1 if intertype == 0 & lastpostinterval!=.

* Create first non-risk period from study start
preserve
keep if intertype == 0 & lastpostinterval==.
tempfile lastnonriskperiod
save `lastnonriskperiod'
unique patid
restore

replace interstart = st_st if intertype == 0 & lastpostinterval==.
replace interend = nextpreinterval - 1 if intertype == 0 & lastpostinterval==.
drop if interstart> interend & intertype == 0 

append using `lastnonriskperiod'

* Create last non-risk period until study end
sort patid interstart
drop lastpostinterval nextpreinterval
replace interstart = interend[_n-1] + 1 if intertype == 0 & interstart ==.
replace interend = st_en if intertype == 0 & interend ==.

* Drop intervals that are not valid
drop if interstart > interend
drop if interend < st_st
replace interend = st_en if interend > st_en
replace interstart = st_st if interstart <st_st
drop if interstart > interend
drop if interend < st_st

assert interstart <= st_en 
assert interend >= st_st

* Handle pre-risk periods when they overlap with last post-risk period
* In favor of keeping post-risk period than pre-risk period
* Sort by interstart interned initially
sort patid interstart interend
bysort patid: gen lastpost_st = interstart[_n-1]
bysort patid: gen lastpost_en = interend[_n-1]
format lastpost_st lastpost_en %td

* Remove pre-risk period if it completely overlap with last post-risk period
drop if intertype == 1 & interstart < = lastpost_st & lastpost_st!=.

* Edit the pre-risk period if it partially overlap with last post-risk period
replace interstart = lastpost_en + 1 if intertype == 1 &  lastpost_st < = interstart & interstart < = lastpost_en & lastpost_en!=.

drop if interstart > interend & intertype == 1

drop lastpost_st lastpost_en

* Resort the dataset as some pre-risk period end date occur before the end date of last post-risk period
* Then repeat the same procedures as above
gsort patid interstart -interend
bysort patid: gen lastpost_st = interstart[_n-1]
bysort patid: gen lastpost_en = interend[_n-1]
format lastpost_st lastpost_en %td

drop if intertype == 1 & interstart < = lastpost_st & lastpost_st!=. 
replace interstart = lastpost_en + 1 if intertype == 1 &  lastpost_st < = interstart & interstart < = lastpost_en & lastpost_en!=.

drop if interstart > interend & intertype == 1

drop lastpost_st lastpost_en

* Edit the last post-risk period after cleaning the pre-risk period as above
sort patid interstart interend
bysort patid: gen nextpost_st = interstart[_n+1]
bysort patid: gen nextpost_en = interend[_n+1]

format nextpost_st nextpost_en %td

replace interend = nextpost_st - 1

drop nextpost_st nextpost_en

* Handle pre-risk periods when they overlap with last exposure risk period
* In favor of keeping exposure risk period than pre-risk period
sort patid interstart interend
bysort patid: gen lastpost_st = interstart[_n-1]
bysort patid: gen lastpost_en = interend[_n-1]
format lastpost_st lastpost_en %td

* Remove pre-risk period if it completely overlap with last exposure period
drop if intertype == 1 & interstart < = lastpost_st  & lastpost_st!=.

* Edit the pre-risk period if it partially overlap with last exposure period
replace interstart = lastpost_en + 1 if intertype == 1 &  lastpost_st < = interstart & interstart < = lastpost_en  & lastpost_en!=.

drop if interstart > interend & intertype == 1

drop lastpost_st lastpost_en

* Resort the dataset as some pre-risk period end date occur before the end date of last post-risk period
* Then repeat the same procedures as above
gsort patid interstart -interend
bysort patid: gen lastpost_st = interstart[_n-1]
bysort patid: gen lastpost_en = interend[_n-1]
format lastpost_st lastpost_en %td

drop if intertype == 1 & interstart < = lastpost_st & lastpost_st!=.
replace interstart = lastpost_en + 1 if intertype == 1 &  lastpost_st < = interstart & interstart < = lastpost_en & lastpost_en!=.

drop if interstart > interend & intertype == 1

drop lastpost_st lastpost_en

* Edit the last post-risk period after cleaning the pre-risk period as above
sort patid interstart interend
bysort patid: gen nextpost_st = interstart[_n+1]
bysort patid: gen nextpost_en = interend[_n+1]

format nextpost_st nextpost_en %td

replace interend = nextpost_st - 1

drop nextpost_st nextpost_en

* Edit the last non-risk window for the end date
replace interend = st_en if interend == . & intertype == 0

* Edit the first start date of episode for each person as st_st and the last end date of episode as st_en
bysort patid: gen indiv_order = _n
bysort patid: gen total_episode = _N
replace interstart = st_st if indiv_order == 1
bysort patid: replace interend = st_en if _n == _N

* Validity checks
assert interstart <= interend
bysort patid exp_st interstart: assert _n == 1
assert interstart == floor(interstart)

sort patid interstart interend
bysort patid: gen nextpost_st = interstart[_n+1]
format nextpost_st %td

assert interend + 1 == nextpost_st if nextpost_st != .

sort patid interstart interend
bysort patid: gen lastpost_en = interend[_n-1]
format lastpost_en %td

assert lastpost_en + 1 == interstart if lastpost_en != .

* Save relevant data
keep patid intertype st_st_old st_en_old interstart_old interend_old st_st st_en interstart interend dob drugexposed
order patid dob st_st_old st_en_old interstart_old interend_old st_st st_en drugexposed intertype interstart interend
save temp_exposure_intervals_`num_interval'.dta, replace
	
}

*append all drug exposed and nonexposed intervals
use temp_exposure_intervals_1.dta, clear
forval num_interval = 2/`max' {
append using temp_exposure_intervals_`num_interval'.dta
}
save temp_exposure_intervals, replace

/*****************************************************************
* 4. Create season intervals
*****************************************************************/
use temp_exposure_intervals.dta, clear

drop st_st st_en
rename st_st_old st_st
rename st_en_old st_en

* Divide the intervals by season
* Handle overlapping period between date of season and intervals
forval year =  $studystartyear/$studyendyear {
gen winter_`year'=mdy(12,01,`year')
gen spring_`year'=mdy(03,01,`year')
gen summer_`year'=mdy(06,01,`year')
gen autumn_`year'=mdy(09,01,`year')
format winter_`year' spring_`year' summer_`year' autumn_`year' %td
}

gen keepflag = .
forval year = $studystartyear/$studyendyear {
	foreach season in spring summer autumn winter {
replace keepflag = 1 if (interstart<=`season'_`year' & `season'_`year'<interend)
	}
}

preserve
keep if keepflag == 1
drop keepflag
save period_overlap, replace
restore

keep if keepflag == .
drop keepflag
save period_nonoverlap, replace

use period_overlap, clear

forval year =  $studystartyear/$studyendyear {
	foreach season in spring summer autumn winter {

preserve
replace interend=`season'_`year' - 1 if interstart<=`season'_`year' & `season'_`year'<interend
save `season'_`year'_partI, replace
restore

replace interstart=`season'_`year' if interstart<=`season'_`year' & `season'_`year'<interend

append using `season'_`year'_partI

bysort patid interstart interend: keep if _n == 1
}
}

* Add non-overlap period
append using period_nonoverlap

* Remove unnecessary files
forval year = $studystartyear/$studyendyear {
	foreach season in spring summer autumn winter {
		erase `season'_`year'_partI.dta
	}
}

* Validity checks
replace interstart = st_st if interstart < st_st & interend >= st_st
drop if interstart > st_en | interend < st_st
replace interend = st_en if interend > st_en & interstart <= st_en
drop if interend < interstart 
 
drop winter* summer* autumn* spring*

sort patid interstart interend

bysort patid interstart: assert _n == 1
assert interstart == floor(interstart)

sort patid interstart interend
bysort patid: gen nextpost_st = interstart[_n+1]
format nextpost_st %td

assert interend + 1 == nextpost_st if nextpost_st != .

sort patid interstart interend
bysort patid: gen lastpost_en = interend[_n-1]
format lastpost_en %td

assert lastpost_en + 1 == interstart if lastpost_en != .

drop nextpost_st lastpost_en

* Create season variable
gen season = 1 if month(interstart) == 12 | ///
					month(interstart) == 1 | ///
					month(interstart) == 2
replace season = 2 if month(interstart) == 3 | ///
					month(interstart) == 4 | ///
					month(interstart) == 5
replace season = 3 if month(interstart) == 6 | ///
					month(interstart) == 7 | ///
					month(interstart) == 8
replace season = 4 if month(interstart) == 9 | ///
					month(interstart) == 10 | ///
					month(interstart) == 11

label variable season "Season category"
label define seasonlbl 1 "Winter" 2 "Spring" 3 "Summer" 4 "Autumn"
label values season seasonlbl

keep patid intertype interstart_old interend_old st_st st_en interstart interend dob drugexposed season
order patid dob st_st st_en interstart_old interend_old drugexposed intertype interstart interend season

save temp_season_intervals.dta, replace

/*****************************************************************
* 5. Create age categorical intervals (5 years age band)
*****************************************************************/

* Divide the intervals by age in 5 year band
* Handle overlapping period between date of current age and intervals

gen birth_month = month(dob)
gen birth_day = day(dob)

forval i = $studystartyear/$studyendyear {
gen birthyear`i'=mdy(birth_month,birth_day,`i')
format birthyear`i' %td
}

* Create age variable
gen age = year(interstart) - year(dob)
assert age != .

su age, detail 

gen keepflag = .
forval year =  $studystartyear/$studyendyear {
replace keepflag = 1 if (interstart<=birthyear`year' & birthyear`year'<interend) ///
 & (age == 22 | age == 27 | age == 32 | age == 37 | age == 42 | age == 47 ///
 | age == 52 | age == 57 | age == 62 | age == 67 | age == 72 | age == 77 ///
 | age == 82 | age == 87 | age == 92 | age == 110)
	}

preserve
keep if keepflag == 1
drop keepflag
save period_overlap, replace
restore

keep if keepflag == .
drop keepflag
save period_nonoverlap, replace

use period_overlap, clear

forval year =  $studystartyear/$studyendyear {
preserve
replace interend=birthyear`year' - 1 if interstart<=birthyear`year' & ///
birthyear`year'<interend
save birthyear`year'_partI, replace
restore

replace interstart=birthyear`year' if interstart<=birthyear`year' & birthyear`year'<interend

append using birthyear`year'_partI

bysort patid interstart interend: keep if _n == 1
}

* Add non-overlap period
append using period_nonoverlap

* Remove unnecessary files
forval year =  $studystartyear/$studyendyear {
		erase birthyear`year'_partI.dta
	}

* Validity checks
replace interstart = st_st if interstart < st_st & interend >= st_st
drop if interstart > st_en | interend < st_st
replace interend = st_en if interend > st_en & interstart <= st_en
drop if interend < interstart 
 
drop birthyear*

sort patid interstart interend

bysort patid interstart: assert _n == 1
assert interstart == floor(interstart)

sort patid interstart interend
bysort patid: gen nextpost_st = interstart[_n+1]
format nextpost_st %td

assert interend + 1 == nextpost_st if nextpost_st != .

drop nextpost_st

sort patid interstart interend
bysort patid: gen lastpost_en = interend[_n-1]
format lastpost_en %td

assert lastpost_en + 1 == interstart if lastpost_en != .

* Create new age variable
gen newage = year(interstart) - year(dob)
assert newage != .

* Create age variable for 5 years band
gen age_gp = 1 if 18 <= newage & newage <= 22
replace age_gp = 2 if 23 <= newage & newage <= 27
replace age_gp = 3 if 28 <= newage & newage <= 32
replace age_gp = 4 if 33 <= newage & newage <= 37
replace age_gp = 5 if 38 <= newage & newage <= 42
replace age_gp = 6 if 43 <= newage & newage <= 47
replace age_gp = 7 if 48 <= newage & newage <= 52
replace age_gp = 8 if 53 <= newage & newage <= 57
replace age_gp = 9 if 58 <= newage & newage <= 62
replace age_gp = 10 if 63 <= newage & newage <= 67
replace age_gp = 11 if 68 <= newage & newage <= 72
replace age_gp = 12 if 73 <= newage & newage <= 77
replace age_gp = 13 if 78 <= newage & newage <= 82
replace age_gp = 14 if 83 <= newage & newage <= 87
replace age_gp = 15 if 88 <= newage & newage <= 92
replace age_gp = 16 if 93 <= newage & newage <= 110

assert age_gp != .

keep patid intertype interstart_old interend_old st_st st_en interstart interend dob drugexposed season age_gp
order patid dob st_st st_en interstart_old interend_old drugexposed intertype interstart interend season age_gp

save "$datafile/psychiatric_drug_intervals", replace

** Close log file
cap log close
