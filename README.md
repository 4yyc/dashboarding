
## get_wegovy_clients_measurements
Description: Measurement Extraction for meds pick-up Visits and Post-Visit Follow-Up

This query retrieves, for each client who receives a Wegovy prescription, the measurement data associated with each 拿藥 (medication pick-up) visit, and supplements it with an additional follow-up measurement taken approximately four weeks after the client’s most recent order date.

The logic operates in three major steps:

1. Collect Measurement Data (InBody + SoftBio)

The query first extracts all available InBody and SoftBio records:

InBody fields:

weight

body fat mass

body fat percentage

waist circumference

BMI

muscle mass
(All values are validated and converted to numeric when possible.)

SoftBio field:

HbA1c
Values that are non-numeric or malformed are preserved with an appended “（待釐清）” tag.

2. Link Measurements to Each Medication Outgoing Order (拿藥紀錄)

For every Wegovy dispensing event, the query identifies:

the order date

the client associated with the order

the closest InBody record relative to that date

the closest SoftBio HbA1c record relative to the same date

This produces a row for every medication pick-up visit, together with the measurement values most relevant to that specific visit date.

Time proximity is computed using the absolute difference in timestamp (via EXTRACT(EPOCH …)), ensuring the closest measurement is selected regardless of whether it occurred before or after the order date.

3. Add a Follow-Up Measurement Row at “Last Order Date + 28 Days”

For each client:

Determine the latest order date.

Compute a target follow-up date = last_order_date + 28 days.

Find:

the InBody record closest to that target date

the SoftBio HbA1c record closest to that target date

A synthetic row is added to the output for each client, containing:

client_id

target date (treated as 訂單日期)

best-matching InBody measurements

best-matching SoftBio results

a row_type flag set to "last_order_plus_28d"

This row represents the client's condition approximately four weeks after their most recent medication visit and can be used for treatment effect evaluation or post-prescription follow-up analysis.

Outcome

The final dataset includes:

One row per medication pick-up visit
with nearest associated InBody and SoftBio data.

One additional follow-up row per client
representing measurements closest to last order date + 28 days.

This structure supports:

longitudinal monitoring

before/after comparisons

clinical outcome evaluation (e.g., weight change, HbA1c improvement)

dashboards tracking medication effectiveness

research analyses requiring standardized follow-up points
