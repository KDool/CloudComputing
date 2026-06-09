# Analytics Logic README

This document explains the logic of the Hadoop MapReduce example contained in `map-reduce/src/main/java/it/unipi/cloud/ChicagoCrimesApp.java`.

## Job 1: aggregate crime counts by type and location

### Purpose
Job 1 computes aggregate crime metrics for each combination of crime `primaryType` and crime `location`.

### Input
Raw CSV rows from the Chicago crimes dataset.

Example input row:
```text
...,THEFT,...,"STREET",true,...
```

### Mapper output
Each row is converted into a composite key/value pair:
- key: `primaryType|location`
- value: `(arrestCount, totalCount)`

Example mapper output:
```text
("THEFT|STREET", ArrestMetricsWritable(1, 1))
```

### Reduce logic
- Combine all values for the same `primaryType|location` key.
- Sum the arrest counts and total incident counts.

Example reduced output:
```text
("THEFT|STREET", ArrestMetricsWritable(20, 50))
```
This means 50 incidents of THEFT at STREET, with 20 arrests.

## Job 2: find the location with the lowest arrest rate for each crime type

### Purpose
Job 2 analyzes the aggregated results from Job 1 and selects the location with the lowest arrest rate for each crime type.

### Input
Job 1 output lines such as:
```text
THEFT|STREET	20,50
THEFT|SIDEWALK	5,60
ROBBERY|ALLEY	10,30
```

### Mapper output
Each line is re-keyed by `primaryType` only and emits a value containing the location and metrics:
- key: `primaryType`
- value: `location@@@arrests,totals`

Example mapper output:
```text
("THEFT", "STREET@@@20,50")
("THEFT", "SIDEWALK@@@5,60")
("ROBBERY", "ALLEY@@@10,30")
```

### Reduce logic
For each crime type:
- parse location, arrests, and totals
- compute arrest rate = `(arrests / totals) * 100`
- ignore locations with `totals <= 10`
- select the location with the smallest arrest rate
- accumulate total incidents for the crime type

### Example final output
For `THEFT`:
- STREET arrest rate = `20 / 50 = 40.00%`
- SIDEWALK arrest rate = `5 / 60 = 8.33%`

Selected worst location:
```text
THEFT	Worst Location: SIDEWALK ... | Total Incidents of this type: 110
```

## How the two jobs cascade

1. Job 1 reads raw CSV and creates intermediate aggregated metrics by `primaryType|location`.
2. Job 2 reads that intermediate data and performs further analytics by crime type.
3. This is a two-stage workflow:
   - raw data → Job 1 aggregation → intermediate output
   - intermediate output → Job 2 analysis → final report

This pipeline structure is common in MapReduce analytics: first compute aggregate values, then analyze aggregated results to produce the final insight.