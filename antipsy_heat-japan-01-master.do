/************************************************
************************************************
**************************************************
Main analysis for antipsychotics
************************************************
************************************************
************************************************/

do "$pathDofiles/1_antipsy_heat-japan-01-set-up-linkage"
do "$pathDofiles/2_antipsy_heat-japan-01-import-extract-data"

*psychiatric patients
*prepare for SCCS analysis
do "$pathDofiles/3_psychiatric_heat-japan-01-set-up-SCCS"
do "$pathDofiles/4_psychiatric_heat-japan-01-prep_sccs-program"
do "$pathDofiles/5_psychiatric_noantipsy_heat-japan-01-prep_sccs-program"

*depression patients
do "$pathDofiles/3_depression_heat-japan-01-set-up-SCCS"
do "$pathDofiles/4_depression_heat-japan-01-prep_sccs-programm"
do "$pathDofiles/5_depression_noantipsy_heat-japan-01-prep_sccs-program"

/************************************************
*Run SCCS heat-related illness outcome
************************************************/
global logname psychiatric_heat-japan-01-heat_illness
global outcomefile $datafile/antipsy_heat_illness_multiple
global output_data $datafile/psychiatric_sccs_heat_illness

do "$pathDofiles/6_psychiatric_heat-japan-01-prep_sccs-program_interaction"

/************************************************
*Run SCCS myocardial infarction outcome
************************************************/
global logname psychiatric_heat-japan-01-MI
global outcomefile $datafile/antipsy_myocardial_infarct_multiple
global output_data $datafile/psychiatric_sccs_myocardial_infarct

do "$pathDofiles/6_psychiatric_heat-japan-01-prep_sccs-program_interaction"

/************************************************
*Run SCCS delirium outcome
************************************************/
global logname psychiatric_heat-japan-01-delirium
global outcomefile $datafile/antipsy_delirium_multiple
global output_data $datafile/psychiatric_sccs_delirium

do "$pathDofiles/6_psychiatric_heat-japan-01-prep_sccs-program_interaction"

/************************************************
************************************************
************************************************
Individual antipsychotics - 5 commonly used antipsychotics
************************************************
************************************************
************************************************/
do "$pathDofiles/8_antipsy_heat-japan-01-stratify-antipsy-set-up-SCCS"

/************************************************
*Run SCCS heat-related illness outcome
************************************************/
global logname individual_antipsy_heat-japan-01-heat_illness
global outcomefile $datafile/antipsy_heat_illness_multiple
do "$pathDofiles/9_antipsy_heat-japan-01-stratify_antipsy_prep_sccs-program"

/************************************************
************************************************
************************************************
Main analysis for antidepressants
************************************************
************************************************
************************************************/
/************************************************
*Run SCCS heat-related illness outcome
************************************************/
global logname depress_heat-japan-01-heat_illness
global outcomefile $datafile/antipsy_heat_illness_multiple
global output_data $datafile/depress_sccs_heat_illness

do "$pathDofiles/6_depression_heat-japan-01-prep_sccs-program_interaction"

/************************************************
*Run SCCS myocardial infarction outcome
************************************************/
global logname depress_heat-japan-01-MI
global outcomefile $datafile/antipsy_myocardial_infarct_multiple
global output_data $datafile/depress_sccs_myocardial_infarct

do "$pathDofiles/6_depression_heat-japan-01-prep_sccs-program_interaction"

/************************************************
*Run SCCS delirium outcome
************************************************/
global logname depress_heat-japan-01-delirium
global outcomefile $datafile/antipsy_delirium_multiple
global output_data $datafile/depress_sccs_delirium

do "$pathDofiles/6_depression_heat-japan-01-prep_sccs-program_interaction"

/************************************************
************************************************
************************************************
Individual antidepressants - 5 commonly used antidepressants
************************************************
************************************************
************************************************/
do "$pathDofiles/8_antidepress_heat-japan-01-stratify-antidepress-set-up-SCCS"

/************************************************
*Run SCCS heat-related illness outcome
************************************************/
global logname individual_depress_heat-japan-01-heat_illness
global outcomefile $datafile/antipsy_heat_illness_multiple

do "$pathDofiles/9_antidepress_heat-japan-01-stratify_antidepress_prep_sccs-program"


/************************************************
************************************************
************************************************
*Sensitivity analysis for antipsychotics
using first recorded outcome only
************************************************
************************************************
************************************************/
/************************************************
*Run SCCS heat-related illness outcome
************************************************/
*any antipsychotics
global logname psychiatric_heat-japan-01-heat_illness_sen
global outcomefile $datafile/antipsy_heat_illness_first

do "$pathDofiles/6_psychiatric_heat-japan-01-prep_sccs-program_interaction_sens"

/************************************************
*Run SCCS myocardial infarction outcome
************************************************/
*any antipsychotics
global logname psychiatric_heat-japan-01-MI_sen
global outcomefile $datafile/antipsy_myocardial_infarct_first

do "$pathDofiles/6_psychiatric_heat-japan-01-prep_sccs-program_interaction_sens"

/************************************************
*Run SCCS delirium outcome
************************************************/
*any antipsychotics
global logname psychiatric_heat-japan-01-delirium_sen
global outcomefile $datafile/antipsy_delirium_first

do "$pathDofiles/6_psychiatric_heat-japan-01-prep_sccs-program_interaction_sens"

/************************************************
************************************************
************************************************
Sensitivity analysis for antidepressants
************************************************
************************************************
************************************************/
/************************************************
*Run SCCS heat-related illness outcome
************************************************/
*any antidepressants
global logname depress_heat-japan-01-heat_illness_sen
global outcomefile $datafile/antipsy_heat_illness_first

do "$pathDofiles/6_depression_heat-japan-01-prep_sccs-program_interaction_sens"

/************************************************
*Run SCCS myocardial infarction outcome
************************************************/
*any antidepressants
global logname depress_heat-japan-01-MI_sen
global outcomefile $datafile/antipsy_myocardial_infarct_first

do "$pathDofiles/6_depression_heat-japan-01-prep_sccs-program_interaction_sens"

/************************************************
*Run SCCS delirium outcome
************************************************/
*any antidepressants
global logname depress_heat-japan-01-delirium_sen
global outcomefile $datafile/antipsy_delirium_first

do "$pathDofiles/6_depression_heat-japan-01-prep_sccs-program_interaction_sens"

/************************************************
************************************************
************************************************
*Case crossover study
************************************************
************************************************
************************************************/
do "$pathDofiles/10_mental_heat-call-mdel-01-CCO-set-up"

*psychotropic drug and individual drug CCO
do "$pathDofiles/11_psychotropic_heat-call-mdel-01-CCO-set-up"


/************************************************
************************************************
************************************************
*descriptive analysis
************************************************
************************************************
************************************************/
do "$pathDofiles/7_antipsy_heat-japan-01-all-antipsy-descriptive"
do "$pathDofiles/7_antidepress_heat-japan-01-all-antidepress-descriptive"

/************************************************
************************************************
************************************************
*sensitivity analysis removing people without outpatient visits during study period
************************************************
************************************************
************************************************/
/************************************************
*Run SCCS heat-related illness outcome
************************************************/
global logname psychiatric_heat-japan-01-heat_illness-sens-outpatient
global output_data $datafile/psychiatric_sccs_heat_illness

do "$pathDofiles/15_heat-japan-01-sensitivity-analysis-no-outpatient"

/************************************************
*Run SCCS myocardial infarction outcome
************************************************/
global logname psychiatric_heat-japan-01-MI-sens-outpatient
global output_data $datafile/psychiatric_sccs_myocardial_infarct

do "$pathDofiles/15_heat-japan-01-sensitivity-analysis-no-outpatient"

/************************************************
*Run SCCS delirium outcome
************************************************/
global logname psychiatric_heat-japan-01-delirium-sens-outpatient
global output_data $datafile/psychiatric_sccs_delirium

do "$pathDofiles/15_heat-japan-01-sensitivity-analysis-no-outpatient"