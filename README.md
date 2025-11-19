
## get_wegovy_clients_measurements
#### Query Description
This query retrieves comprehensive measurement records for Wegovy-related dispensing visits. It performs two core tasks:

1. **For each dispensing visit (order event):**

   * Fetches the InBody and SoftBio measurement values closest to the visit’s `order_date`.
   * Includes key body composition metrics (weight, body fat mass, body fat percentage, waist circumference, BMI, and muscle mass).
   * Includes laboratory measurement (HbA1c) with safe handling for non-numeric or irregular values.

2. **For each client:**

   * Identifies their latest Wegovy-related order date.
   * Computes a target date equal to `last_order_date + 28 days`.
   * Retrieves the InBody and SoftBio measurements closest to that +28‑day target date.
   * Outputs this as an additional synthetic row to support longitudinal or follow‑up analysis.

The result is a dataset containing:

* One row per actual medication pick-up visit, enriched with the nearest clinical measurements.
* One additional row per client representing the follow-up measurement snapshot around +28 days after their last order.

This structure supports downstream use cases such as treatment effect evaluation, longitudinal health trend tracking, and modeling of short-term response to medication.

