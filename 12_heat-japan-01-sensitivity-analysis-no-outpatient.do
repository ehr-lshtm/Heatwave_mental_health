/*=========================================================================
DO FILE NAME:			12_heat-japan-01-sensitivity-analysis-no-outpatient

AUTHOR:					Angel Wong	
						
VERSION:				v1
					
DATABASE:				Japanese claims data
	
DESCRIPTION OF FILE:	sensitivity analysis to remove people without any outpatient visits in people with severe mental disorder
											
DO FILES NEEDED:	NA

ADO FILES NEEDED: 	NA

MORE INFORMATION:	

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
* 1. generate a dataset only containing patient id that has inpatient but no outpatient records
*****************************************************************/
use "$datafile/FromYuta/231212_typecheck/231212_claims_typecheck_merged.dta", clear
keep if inpatient == 1 & outpatient == 0
rename 加入者id patid
keep patid

save "$datafile/hospital_stay", replace

/*****************************************************************/
* 2. use the data generated previously; exclude people identified in step 1
*****************************************************************/
use "$output_data", clear

merge m:1 patid using "$datafile/hospital_stay", keep(master) keepusing(patid) nogen

/*****************************************************************
* 3. Run SCCS using conditional Poisson regression
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


*age at cohort entry
duplicates drop patid, force
gen age_st = (st_st - dob)/365.25

su age_st, detail

*sex
tab sex, m


** Close log file
log close
