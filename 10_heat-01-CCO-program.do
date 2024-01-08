/*=========================================================================
DO FILE NAME:			10_heat-01-CCO-program

AUTHOR:					Angel Wong
VERSION:				v1
					
DATABASE:				Japanese claims data

Aim: To create datasets for running case-crossover studies 
Steps:

Among the identified cases
1. set up case and control periods for each individual
2. identify exposure in case and control periods
3. generate variables for regression
4. run Conditional logistic regression 
*=============================================================================*/

capture log close

/*******************************************************************************
Identify file locations
*******************************************************************************/
* open log file - no need as fast tool will create log files
log using "$logname", text replace

/**********************************************************************
* Open the dataset
***********************************************************************/
use "$inputdataset", clear

/*1. create hazard window and control windows*/
gen period_end = eventdate
gen period_start = eventdate - $riskperiod + 1

*create 2 obs per patient and create a period variable
expand 2, generate(period)

replace period_end = eventdate - $riskperiod - $washoutperiod + 2 if period == 0
replace period_start = period_end - $riskperiod + 1  if period == 0

format period_end period_start %td

label drop _merge
label def _merge 0 "Control window" 1 "Hazard window"

sort patid period_start

*drop patients if their risk periods were not within the valid study period
gen exclude = 1 if period_start < cohortentry 
bysort patid: egen max_exclude = max(exclude)
drop if max_exclude == 1
drop exclude max_exclude

/*2. identify heat exposure in risk periods key by city code category*/

* add exposure variable
joinby city_cat_code using "$exposure_dataset", unmatched(master)
gen exposure = 1 if period_start<=hw_start & hw_start<=period_end 
replace exposure = 1 if period_start<=hw_end & hw_end<=period_end 
replace exposure = 0 if exposure == .

gsort patid period -exposure
bysort patid period: keep if _n == 1

drop hw_start hw_end _merge

* Remove people who do not have discordant pairs of exposure between periods
bysort patid: egen max_exposure= max(exposure)
bysort patid: egen min_exposure= min(exposure)

drop if min_exposure== 0 & max_exposure== 0
drop if min_exposure== 1 & max_exposure== 1

drop max_exposure min_exposure

* Number of event
preserve
duplicates drop patid, force
count
local event = r(N)
restore

* Run model
clogit period i.exposure, group(patid) or

* Output to text file
cap file close tablecontent
file open tablecontent using "$outputtable", write text replace

file write tablecontent _tab ("Exposure")  _n
						
file write tablecontent ("$drug") _tab ///
						 ("HR") _tab ("95% CI") _n
cap {						
lincom 1.exposure, eform 			
file write tablecontent _tab %4.2f (r(estimate)) _tab %4.2f (r(lb)) (" - ") %4.2f (r(ub)) _n
}

file write tablecontent _tab ("Total N") _n
file write tablecontent _tab (`event')

file write tablecontent _n
file close tablecontent


log close