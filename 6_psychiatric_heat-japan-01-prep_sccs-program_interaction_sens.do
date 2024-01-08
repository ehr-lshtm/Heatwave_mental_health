/*=========================================================================
DO FILE NAME:			6_psychiatric_heat-japan-01-prep_sccs-program_interaction_sen

AUTHOR:					Angel Wong	
						
VERSION:				v1
					
DATABASE:				Japanese claims data
	
DESCRIPTION OF FILE:	Run SCCS using heat as an exposure and adding the outcome
											
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
log using "${pathLogs}/$logname", text replace
*********************************************************************

/*****************************************************************/
* 1. Create variables for outcomes and offset
*****************************************************************/
use "$datafile/psychiatric_all_intervals_final", clear

joinby patid using "$outcomefile", unmatched(master)
drop icd10 term _merge

* Create outcome variable
replace eventdate = . if eventdate!=. & eventdate > st_en
replace eventdate = . if eventdate!=. & eventdate < st_st

gen outcome_ind = 1 if eventdate >= interstart & eventdate <= interend
replace outcome_ind = 0 if outcome_ind == .

* remove duplicates
gsort patid interstart interend -outcome_ind
bysort patid interstart interend: keep if _n == 1

* remove those who do not have an outcome during observation period
bysort patid: egen max_outcome = max(outcome_ind)
drop if max_outcome == 0
drop max_outcome

* Create variable for interval (offset)
gen interval = interend - interstart + 1
gen loginterval = log(interval)

* View number of event in each risk period
tab outcome_ind intertype,m

assert interstart <= st_en 
assert interend >= st_st

cou if eventdate>st_en & eventdate !=.
cou if eventdate<st_st & eventdate !=.

unique patid

/*****************************************************************
* 6. Run SCCS using conditional Poisson regression
*****************************************************************/
*interaction
xi i.intertype*i.drugexposed i.season i.age_gp
xtpoisson outcome_ind _Iintertype_*  _Idrugexpos_* _IintXdru_* _Iseason_* _Iage_gp_*, fe i(patid) offset(loginterval) irr
est store modA

forval riskperiod =1/3 {
di "Without drug exposure in risk period of `riskperiod'"
lincom _Iintertype_`riskperiod', eform 
}

forval riskperiod =1/3 {
di "With drug exposure in risk period of `riskperiod'"
lincom _Iintertype_`riskperiod' + _IintXdru_`riskperiod'_`category', eform 
}

*interaction test
xtpoisson outcome_ind _Iintertype_*  _Idrugexpos_* _Iseason_* _Iage_gp_*, fe i(patid) offset(loginterval) irr
est store modB

lrtest modA modB

*The program drops only one obs per group (as they don't have both risk window & baseline window) or all zero outcomes
bysort patid: gen count_n = _N
drop if count_n==1
bysort patid: egen max_out = max(outcome_ind)
drop if max_out==0

* Number of people included
unique patid

* View number of event in each risk period
tab outcome_ind intertype,m

* View number of event in each risk period by drug exposed
preserve
di "Without drug exposure"
keep if drugexposed == 0
tab outcome_ind intertype,m
restore

preserve
di "With drug exposure"
keep if drugexposed == 1
tab outcome_ind intertype,m
restore

*person-years in each risk period
bysort intertype drugexposed: egen sum_day = sum(interval)
gen person_yr = sum_day/365.25

bysort intertype drugexposed: su person_yr, detail

** Close log file
log close
