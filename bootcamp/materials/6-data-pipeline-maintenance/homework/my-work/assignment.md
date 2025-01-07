# Week 5 Data Pipeline Maintenance - Assignment

Example is for the “business to business” type of product/company where customers actually represent so-called accounts. For each account licenses are sold and their employees are using or not using licenses, accounts can change contracts and renew all licenses, increase or decrease number of licenses, change types of licenses etc.

## Pipelines

* Profit
  * Unit-level profit needed for experiments
  * Aggregate profit reported to investors
* Growth
  * Daily growth needed for experiments
  * Aggregate growth reported to investors
* Engagement
  * Aggregate engagement reported to investors

## Runbooks

### Pipeline Name: Profit - unit level
* Types of data:
    * Revenue streams
    * Operating expense - Payrolls
    * Operating expense  - Marketing
    * Operating expense  - Insurance
    * Operating expense  - R&D
    * Operating expense  - Rent and inventory
    * Capital expense
    * Financial/profit targets
* Owners:
    * Primary Owners: Profit analysts in Finances team
    * Secondary Owners: Pipeline owner in Data engineering
* Roles:
    * Responsible: Pipeline owner in Data engineering
    * Accountable:  Profit analysts in Finances team
    * Consulted: Unit heads
    * Informed: Data Engineering team manager, Finances team manager
* Common Issues:
    * Numbers differences compared to financial statement or balance cards
    * Marketing data is delayed as significant portion of it represents manual entries of expenses
    * R&D is using various providers depending on the need and not all costs data streams from those providers are integrated on time
    * External provider API for marketing and R&D has changed structure of the data or is no longer available
* SLA’s:
    * Numbers will be reviewed once per month by the finances team and unit heads
    * Warning for external API data if not available for 24h, Alarm if not available for 48h, problems need to be resolved within a week
* On-call schedule:
    * Monitored by the responsible in the finances team on a weekly level to identify if something is broken or requires to be fixed.
    * Data engineers will fix reported problems on a monthly level within regular working hours
    * On-call data engineers are rotated on a weekly level.
    * Data engineers will monitor for API problems on a weekly level and fix API problems asap

### Pipeline Name: Aggregate profit reported to investors
* Types of data:
    * Profit data
* Owners:
    * Primary Owners: Profit analyst in Business strategy and analytics team
    * Secondary Owners: Pipeline owner in Data engineering
* Roles:
    * Responsible: Pipeline owner in Data engineering
    * Accountable:  Profit analyst in Business strategy and analytics team
    * Consulted: Profit analysts in Finances team, Unit heads
    * Informed: Data Engineering team manager, Business strategy and analytics team manager
* Common Issues:
    * Broken pipeline in the upstream
        * External provider API has changed structure of the data or is no longer available
        * Revenue stream data is large and detailed and can lead to OOM
    * Stale data in upstream pipeline impacting backfilling of the aggregated data
* SLA’s:
    * Reports need to be available on the last day of the month, until when all problems need to be fixed
    * Warning for external API data if not available for 24h, Alarm if not available for 48h, problems need to be resolved within a week
* On-call schedule:
    * Data engineers will monitor pipeline in the last week of the month and fix problems if they occur
    * In case of holidays in the last week, on call data engineer will be assigned, within regular working hours (no on-night call)


### Pipeline Name: Daily growth needed for experiments
* Types of data:
    * Accounts information
        * Estimated size
        * Regional/Geographical information
        * Business category
        * Partnership level
        * Account status
    * Product and revenue information for accounts
        * Change in the number of licenses
        * Current number of licenses
        * Type of licenses
        * Product lines / modules in use
        * Date for renewal of licenses
        * Revenue in total for the current year
        * Revenue change on daily level
    * Engagement data
        * Number of monthly/weekly/daily active users
        * Activity time
        * Activity profile
    * Technical support information
        * Number of support tickets raised
        * Severity of tickets
       *  Resolution time
    * Marketing data
        * Information about marketing campaigns - events/channels
* Owners:
    * Primary Owners: Growth analyst in Sales and growth team, Front-end developer for user activity in Software development team
    * Secondary Owners: Pipeline owner in Data engineering
* Roles:
    * Responsible: Pipeline owner in Data engineering
    * Accountable:  Growth analyst in Sales and growth team
    * Consulted: Growth analyst in Sales and growth team, Front-end developer for user activity in Software development team, Technical support team manager
    * Informed: Data Engineering team manager, Sales and growth team manager
* Common Issues:
    * Technical support platform provider API has changed structure of the data or is no longer available
    * Missing or outdated information about the account(s)
   *  Missing or outdated marketing information
    * Engagement data issues due to streaming platform problems
        * Streaming platform was down leading to missing/stale data
        * Events arrive late and are not included in the aggregated data
        * Events are received multiple times resulting in the need to deduplicate the data
* SLA’s:
    * Correctness of the data needs to be assured on a weekly level
    * Warning for engagement data if not available for 24h, Alarm if not available for 48h, problems need to be resolved within a week
* On-call schedule:
    * Not critical, therefore no on-call, problems are resolved on a weekly level within regular working time


### Pipeline Name: Aggregate growth reported to investors
* Types of data:
    *  Growth data
* Owners:
    * Primary Owners: Growth analyst in Business strategy and analytics team
    * Secondary Owners: Pipeline owner in Data engineering
* Roles:
    * Responsible: Pipeline owner in Data engineering
    * Accountable:  Growth analyst in Business strategy and analytics team
    * Consulted: Growth analyst in Sales and growth team, Front-end developer for user activity in Software development team
    * Informed: Data Engineering team manager, Business strategy and analytics team manager
* Common Issues:
    * Broken pipeline in the upstream
        * Product and revenue stream and engagement data is large and detailed and can lead to OOM
    * Stale data in upstream pipeline impacting backfilling of the aggregated data
* SLA’s:
    * Reports need to be available on the last day of the month, until when all problems need to be fixed
    * Warning for engagement data if not available for 24h, Alarm if not available for 48h, problems need to be resolved within a week
* On-call schedule:
    * Data engineers will monitor pipeline in the last week of the month and fix problems if they occur
    * In case of holidays in the last week, on call data engineer will be assigned, within regular working hours (no on-night call)


### Pipeline Name: Aggregate engagement reported to investors
* Types of data:
    * Accounts information
        * Estimated size
        * Regional/Geographical information
        * Business category
        * Partnership level
        * Account status
    * Engagement data
        * Number of monthly/weekly/daily active users
        * Activity time
        * Activity profile
* Owners:
    * Primary Owners: Product engagement analyst in Business strategy and analytics team
    * Secondary Owners: Pipeline owner in Data engineering
* Roles:
    * Responsible: Pipeline owner in Data engineering
    * Accountable:  Product engagement analyst in Business strategy and analytics team
    * Consulted: Front-end developer for user activity in Software development team
    * Informed: Data Engineering team manager, Business strategy and analytics team manager
* Common Issues:
  * Engagement data issues due to streaming platform problems
    * Streaming platform was down leading to missing/stale data
    * Events arrive late and are not included in the aggregated data
    * Events are received multiple times resulting in the need to deduplicate the data
  * Engagement data is large and detailed and can lead to OOM
* SLA’s:
  * Reports need to be available on the last day of the month, until when all problems need to be fixed
  * Warning for engagement data if not available for 24h, Alarm if not available for 48h, problems need to be resolved within a week
* On-call schedule:
  * Data engineers will monitor pipeline in the last week of the month and fix problems if they occur.
  * Due to the nature of product, holidays are not critical to be included, therefore report and data engineer's activity will be planned accordingly in the last week of the month.
