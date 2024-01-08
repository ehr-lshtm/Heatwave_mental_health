/*=========================================================================
DO FILE NAME:			8_antidepress_heat-japan-01-stratify-antidepress-set-up-SCCS

AUTHOR:					Angel Wong	
						
VERSION:				v1
					
DATABASE:				Japanese claims data
	
DESCRIPTION OF FILE:	Identify study population using heat as an exposure in people prescribed antidepressants N06A

MORE INFORMATION:	

Identify first case within the observation period

Observation period defined as
start from the first day of antidepressant, study start date (1/1/2014) and end at death date or study end date 31/12/2021, treatment discontinuation (Defined as >90 days gaps between Rx)
Exclude people with age <18 at the study start date
*=========================================================================*/

cap log close

* open log file - no need as fast tool will create log files
log using "${pathLogs}/8_antidepress_heat-japan-01-stratify-antidepress-set-up-SCCS", text replace

/*************************************
*Antidepressant users (take treatment discontinuation into account)
**************************************/
use "$datafile/psychotropic_drug_N06A", clear
unique 加入者id
drop if year(rxst) < 2014 | year(rxst) > 2021

rename 加入者id patid

*Handle prescription gaps
sort patid rxst rxen
keep patid rxst rxen
drop if rxst > rxen

by patid: gen episode=_n

rename rxst timerxst
rename rxen timerxen

*Start the steps of handling overlapping by reshaping the data
reshape long time, i(patid episode) j(start_end) string

*Encode the start and end for ranking the order for "rxst" first
gen start_end2= 0 if start_end=="rxst"
replace start_end2=1 if start_end=="rxen"

by patid (time start_end2), sort: gen int in_proc = sum(start_end == "rxst") - sum(start_end == "rxen")
replace in_proc = 1 if in_proc > 1
by patid (time): gen block_num = 1 if in_proc == 1 & in_proc[_n-1] != 1
by patid (time): replace block_num = sum(block_num)

by patid block_num (time), sort: assert start_end == "rxst" if _n == 1
by patid block_num (time): assert start_end == "rxen" if _n == _N
by patid block_num (time): keep if _n == 1 | _n == _N

drop episode in_proc start_end2
reshape wide time, i(patid block_num) j(start_end) string
rename time* *
order rxst, before(rxen)

by patid: gen episode=_n
keep patid episode rxst rxen

rename rxst timerxst
rename rxen timerxen

reshape long time, i(patid episode) j(start_end) string

gen start_end2= 0 if start_end=="rxst"
replace start_end2=1 if start_end=="rxen"

by patid (time start_end2), sort: gen gap_num = 1 if start_end == "rxst" & (time- time[_n-1]<=1) //change the number of days here
replace gap_num = 1 if start_end == "rxen" & gap_num[_n+1] == 1
egen gap_num_max=max(gap_num), by (patid episode)

keep if (gap_num_max==1 & gap_num==.) | (gap_num==. & gap_num_max ==.)

drop gap_num gap_num_max episode start_end2

*change the episode no as rx for reshaping the wide form
egen rx =seq(), f(1) b(2)
reshape wide time, i(patid rx) j(start_end) string
rename time* *
order rxst, before(rxen)

count 

keep patid rxst rxen

*Further combining Rxs for their gap >90 between Rxs 
by patid: gen episode=_n
keep patid episode rxst rxen

rename rxst timerxst
rename rxen timerxen

reshape long time, i(patid episode) j(start_end) string

gen start_end2= 0 if start_end=="rxst"
replace start_end2=1 if start_end=="rxen"

by patid (time start_end2), sort: gen gap_num = 1 if start_end == "rxst" & (time- time[_n-1]<=90) 
replace gap_num = 1 if start_end == "rxen" & gap_num[_n+1] == 1
egen gap_num_max=max(gap_num), by (patid episode)

keep if (gap_num_max==1 & gap_num==.) | (gap_num==. & gap_num_max ==.)

drop gap_num gap_num_max episode start_end2

*change the episode no as rx for reshaping the wide form
egen rx =seq(), f(1) b(2)
reshape wide time, i(patid rx) j(start_end) string
rename time* *
order rxst, before(rxen)

count 

keep patid rxst rxen

sort patid rxst rxen
bysort patid: keep if _n==1

*merge those with the depression diagnostic date
merge m:1 patid using "$datafile/depression_all", keepusing(eventdate) keep(match) nogen

rename patid 加入者id

*set up observation period
bysort 加入者id: egen st_st = min(rxst)
format st_st %td

rename rxst rxst_antipsy
rename rxen rxen_antipsy

*Remove anyone who had antidepressant 180 days before the first prescription
merge 1:m 加入者id using "$datafile/psychotropic_drug_N06A", keepusing(rxst rxen) keep(master match) nogen

gen excl = 1 if rxst!=. & st_st-180 <= rxst & rxst < st_st
bysort 加入者id: egen max_excl = max(excl)

drop if max_excl==1
unique 加入者id

drop rxst rxen max_excl rxst excl

sort 加入者id rxst_antipsy rxen_antipsy
bysort 加入者id: keep if _n==1
unique 加入者id


*flag those who had multiple antidepressants overlapped the start date
merge 1:m 加入者id using "psychotropic_drug_N06A_rd", keepusing(rxst drug_type) keep(master match) nogen
gen ever_taking = 1 if rxst >= rxst_antipsy & rxst <= rxen_antipsy
drop if ever_taking !=1
sort 加入者id drug_type rxst
duplicates drop 加入者id drug_type, force
bysort 加入者id: gen antipsy_ever_num = _N
gen same_day = 1 if rxst == rxst_antipsy
bysort 加入者id: egen antipsy_same_num = total(same_day)
drop ever_taking same_day

sort 加入者id rxst_antipsy rxen_antipsy
bysort 加入者id: keep if _n==1

unique 加入者id

*keep people with age >=18 at the start of follow-up
merge 1:1 加入者id using "$datafile/psychotropic_drug_N06A_hc", ///
keepusing (dob gender ob_st_date ob_end_date city_code death_flag) keep(match) nogen
gen age_index = (st_st-dob)/365.25
drop if age_index <18
unique 加入者id

su st_st, format
gen st_st_new = max(st_st, ob_st_date)
gen st_en = min(ob_end_date, d(31dec2021), rxen_antipsy)
format st_st_new st_en %td

*remove people with invalid observation period
drop if st_en < st_st_new
unique 加入者id 

drop st_st
rename st_st_new st_st

save "$datafile/antidepress_first_clean_drug_all", replace

*key city category code to city code
use "$datafile/antidepress_first_clean_drug_all", clear
merge m:1 city_code using "$datafile/city_codebook_v2", keep(master match) keepusing(city_cat_code) nogen

tab city_cat_code,m

forval num = 1/6 {
	preserve
	keep if city_cat_code == `num'
	save "antidepress_first_clean_drug_all`num'", replace
	restore
}

*key the heatwave data to antidepressant data stratified by 6 cities
forval num = 1/6 {

use "antidepress_first_clean_drug_all`num'", clear

*set up risk period (heatwave)
gen obs=1
joinby obs using "$datafile/MEANT_heatwave_city`num'_date"
gen exposure = 1 if st_st<=hw_start & hw_start<=st_en
replace exposure = 1 if st_st<=hw_end & hw_end<=st_en

*Remove those who did not experience heatwave during observation period
bysort 加入者id: egen max_exposure = max(exposure)
keep if max_exposure == 1

gen exp_st = max(hw_start, st_st)
gen exp_en = min(hw_end, st_en)
format exp_st exp_en %td

drop if exp_st > exp_en

drop obs exposure max_exposure
 
rename 加入者id patid 

save "antidepress_clean_drug_all_heatwave_`num'", replace
}

*combine all dataset into one
use "antidepress_clean_drug_all_heatwave_1", clear
forval num = 2/6 {
append using "antidepress_clean_drug_all_heatwave_`num'"
}
save "$datafile/antidepress_clean_drug_all_heatwave", replace

*erase unneccessary datasets 
forval num = 1/6 {
erase "antidepress_clean_drug_all_heatwave_`num'.dta"
erase "antidepress_first_clean_drug_all`num'.dta"
}

*stratify into 5 commonly used antidepressant
use "$datafile/antidepress_clean_drug_all_heatwave", clear
duplicates drop patid, force
rename patid 加入者id
drop rxst drug_type
merge 1:m 加入者id using "$datafile/psychotropic_drug_N06A", keep(master match) keepusing(drug_type rxst rxen) nogen
drop if rxen<st_st
drop if rxst>st_en
sort 加入者id drug_type
duplicates drop 加入者id drug_type, force
duplicates tag 加入者id, gen(num_psy)
gen cat_num_psy = 0 if num_psy == 0
replace cat_num_psy = 1 if cat_num_psy == .
label var cat_num_psy "Type of antidepressants in the observation period"
label def psy_cat 0 "Only one antidepressants" 1 "More than one antidepressants" 
label val cat_num_psy psy_cat
duplicates drop 加入者id, force

preserve
keep if cat_num_psy == 0
tab drug_type,m sort
restore

gen drug_interest = 1 if drug_type == 20
replace drug_interest = 2 if drug_type == 11
replace drug_interest = 3 if drug_type == 10
replace drug_interest = 4 if drug_type == 17
replace drug_interest = 5 if drug_type == 13
replace drug_interest = 6 if drug_interest == .
replace drug_interest = 6 if cat_num_psy == 1
label var drug_interest "Type of antidepressants of interest in the observation period"
label def drug_int_cat 1 "duloxetine" 2 "setraline" 3 "paroxetine" ///
 4 "mirtazapine" 5 "escitalopram"  6 "Others"

label val drug_interest drug_int_cat
rename 加入者id patid
keep patid cat_num_psy drug_interest
save "$datafile/antidepress_clean_drug_all_heatwave_drugcat", replace

log close
