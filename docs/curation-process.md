This documentation includes all the necessary steps for curating a publication and submitting to cBioPortal.

[Step 1. Create Study Meta](#step-1-create-study-meta)   
[Step 2. Create Clinical Files](#step-2-create-clinical-files)   
[Step 3. Create Mutation Files](#step-3-create-mutation-files)   
[Step 4. Other Data Types](#step-4-other-data-types)   
[Step 5. Case Lists](#step-5-case-lists)   
[Step 6. Wrapping Up](#step-6-wrapping-up)   
[Step 7. Validation](#step-7-validation)   

## Step 1. Create Study Meta

A `meta_study.txt` file is required. Details and examples refer to format [HERE](https://docs.cbioportal.org/5.1-data-loading/data-loading/file-formats#meta-file)
- The `Name` field should strictly follow the format: Tumor Type (Institute, Journal Year) e.g. `brca_mskcc_2015`
- The `Description` field should be a one-line statement covering the seq type, tumor normals, # of samples or patients in any specific type of tumor/sample classification, tumor type.
- `Citation` and `PubMed ID` should be included if the corresponding paper is published.
- Do not include `add_global_case_lists field` in metafile since case lists for all should be created.
- Add the keyword “pediatric” into study name and description, if any pediatric samples are involved. 

## Step 2. Create Clinical Files

### 2.1 create meta files 
`meta_clinical_sample.txt` and `meta_clinical_patient.txt`. For content and format, please follow the format [HERE](https://docs.cbioportal.org/5.1-data-loading/data-loading/file-formats#meta-files)

### 2.2 create patient level clinical file 
`data_clinical_patient.txt`
- For content and format, please follow the format [HERE](https://docs.cbioportal.org/5.1-data-loading/data-loading/file-formats#clinical-patient-columns)
- For each clinical attribute, use [CDD](http://oncotree.mskcc.org/cdd/swagger-ui.html#!/clinical-data-dictionary-controller/getClinicalAttributeMetadataBySearchTermsUsingPOST) as reference for name, description and type

### 2.3 create sample level clinical file 
`data_clinical_sample.txt`. For content and format, please follow the format [HERE](https://docs.cbioportal.org/5.1-data-loading/data-loading/file-formats#clinical-sample-columns)

### 2.4 data sanity check
Once the files are created, a sanity check is required, for consistency, validation and easthetic purpose. 
#### General
   - `Oncotree Code`, `Cancer Type` and `Cancer Type Detailed` columns should be present. `Cancer Type` and `Cancer Type Detailed` values should be in accordance to the ONCOTREE CODE definition [here](http://oncotree.mskcc.org/#/home)
   - Make sure the sample/patient count match the study description
   - Always include matched normal status for all samples with column `SOMATIC_STATUS` added
   - `TMB` column should be added, generated by [this script](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/tmb/calculate_tmb)
#### Header
- Proper capitalization(first letters / camel cases) of the attribute name, description and type. (e.g. `Oncotree Code`) 
- No underscore in attribute name and description
- No hyphen in attribute IDs; use underscore to separate words (e.g. `ONCOTREE_CODE`)
- Gender related attributes should all be normalized to `Sex`
- Survival attributes should be named as [PREFIX]_STATUS and [PREFIX]_MONTHS with the same prefix value.
#### Values
- Normalize `SAMPLE_CLASS`: Can only contain values `Tumor`, `Cell Line`, `Xenograft`
- Normalize `SAMPLE_TYPE`: Can only contain values `Primary`, `Metastasis`, `Recurrence`
- Normalize `SEX`: Can only contain values `Male`, `Female`
- Normalize `SOMATIC_STATUS`: Can only contain values `Matched`, `Unmatched`

## Step 3. Create Mutation Files

### 3.1 create meta file
`meta_mutations.txt`. For content and format, please follow the format [HERE](https://docs.cbioportal.org/5.1-data-loading/data-loading/file-formats#meta-file-6)

### 3.2 For targetted sequenced studies
If study is targetted sequenced, gene panel file(s) `data_gene_panel_(panel_name).txt` and a gene matrix file `data_gene_panel_matrix.txt` are needed. WGS, WES study could skip this step. For content and format, please follow the forat [HERE](https://docs.cbioportal.org/5.1-data-loading/data-loading/file-formats#gene-panel-data)
- Gene Panel files should be added in [reference_data/gene_panels](https://github.com/cBioPortal/datahub/tree/master/reference_data/gene_panels) on datahub
- Calculate the profiled coding base by running [this script](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/tmb/calculate_number_of_profiled_coding_base_pairs), which would add a field in the panel file called `number_of_profiled_coding_base_pairs`

### 3.2 create MAF
`data_mutations.txt`. For content and format, please follow the format [HERE](https://docs.cbioportal.org/5.1-data-loading/data-loading/file-formats#data-file-5)

### 3.3 annotate MAF 
All variants should be annotated with protein change by [Genome Nexus API](https://www.genomenexus.org/swagger-ui.html), using [this wrapper script](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/GN-annotation-wrapper)

### 3.4 MAF sanity check
- The Entrez Gene Id value should either be `0` or empty if unknown.
- Make sure no ‘NA’ for ref alleles (https://github.com/cBioPortal/datahub/issues/621)
- The Reference Build should be GRCh37. Do a liftover if needed. If the value are 37/hg19/NA replace with GRCh37. 
- `Mutated` Issue: make sure no case as ‘deletion/insertions annotated as missense mutations’ exist (https://github.com/cBioPortal/datahub/issues/255)
- Fix cases with `HGVSp_short` annotated as `MUTATED`
- When pushing data subsetted from MSKIMPACT, germline mutations should be removed
- If there are germline mutations, if yes, double check with PI to see if it should be kept; otherwise, by default we don’t include germline mutations in public portal. 
- Correct gene symbols convereted to Dates (SEPT13 -> 13-Sept) by Excel, using [this scirpt](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/hugo-symbol-corrector)

## Step 4. Other Data Types
- For details of how to create meta and data files for other data types, please refer to the [general file format page](https://docs.cbioportal.org/5.1-data-loading/data-loading/file-formats). For how to name the files, please refer to [naming chart](https://github.com/cBioPortal/datahub/blob/master/docs/recommended_staging_filenames.md)
- Every data type/profile should have a meta file associated with it.
- The `stable_id` field in each meta file should be defined using [this chart](https://github.com/cBioPortal/datahub/blob/master/docs/recommended_staging_filenames.md)
- Include the platform used in the profile `description` field of meta Methylation and meta Expression files.

### Expression Data
- Create z-score profiles for each expression profile, using [this script](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/zscores/zscores_relative_allsamples); *double check whether the data is already log transformed* 

## Step 5. Case Lists
- Case lists should always be generated using [this script](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/generate-case-lists)
- Check if the logic of sample numbers is correct in different case lists(e.g. rppa has more sample than all)
- Make sure the number of samples in each case list file corresponds to the sample count/sequenced count from the paper.

## Step 6. Wrapping Up
- Migrate outdated gene symbols in all files using [this script](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/gene-table-update/data-file-migration)
- Make sure all the files are stored as text file in character set: `Unicode(UTF-8)` and `Unix(LF)` *Helpful [articles](https://sites.psu.edu/symbolcodes/software/textfile/)*
- Make sure licence is added to the study folder. For examle, for TCGA data, use [this](https://raw.githubusercontent.com/cBioPortal/datahub/master/public/acc_tcga_pan_can_atlas_2018/LICENSE); for other publication, use [this](https://raw.githubusercontent.com/cBioPortal/datahub/master/public/acyc_fmi_2014/LICENSE)

## Step 7. Validation
- Run validator locally using [this script](https://github.com/cBioPortal/datahub-study-curation-tools/tree/master/validation/validator)
- Create a PR on [cBioPortal Datahub](https://github.com/cBioPortal/datahub). Circle CI would then run automatically. 
- Please make sure validation by Circle CI is passed and then add reviewer(s) to review the PR