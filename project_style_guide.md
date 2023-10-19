# hinge-dbt Project Overview and Style Guide

## Background Notes
When working in dbt, some key terms you will come across are **transformation**, **source**, **model**, **materialization**, **run**, 
**build**, **tests**, and **data model**. These terms can lead to confusion because some of them have very specific meanings in dbt that might differ 
from how you commonly use them. 

* **Transformation** - One of the primary purposes of using dbt is to centralize how, and where we transform data, so that our work is more consistent, repeatable, and 
auditable. Data transformation in this context can mean manipulating, reorganizing, restructuring, converting, renaming, and/or wrangling data in 
such a way to make it easier for us to do dashboarding, reporting, data science, and analysis. Transformations in dbt are based on the concept of ELT 
which stands for extract, load, and transform. What it means is that data has been extracted and loaded to Databricks (responsibility of data engineering), 
and we handle the transformations. So think of dbt as the "T" part of ELT. 95% of the time we will be transforming data in dbt using SQL, and a python-based templating 
language called Jinja.
* **Source** - A source in dbt represents a schema containing tables, and views that has been landed in Databricks for us to work with. The source might be raw data in the sense 
that it comes directly from a source system or application as-is, meaning no transformations have been applied to the data. In other cases a 
source might contain tables or views that have been transformed or aggregated in some way. For example, think of tables in the **rollups** schema. 
One common pattern followed in dbt is to work only with raw data sources, in which case ALL transformations are done in dbt. 
In our Databricks environment, we are working with both raw data sources as well as some data sources that have already been transformed.
Ultimately, we strive to move away from using any rolled-up data so that we are in control of all data transformations. Sources and their corresponding tables and 
views are defined in a .yml configuration file. 
* **Model** - A model in dbt is simply one or more SQL select statements that have been organized in a way to produce 
some data output, i.e. a dataset. It is very important to understand that you will only be writing select statements. 
There is no mechanism in dbt to write your own insert or update statements. Deleting data is an anti-pattern; there is no way to do it. 
Inserts and updates are handled in dbt behind the scenes so to speak, depending on how you materialize a model, which is explained in more detail below. 
Note that you can create Python models in dbt. Python models should mostly be reserved for algorithmic modeling (e.g. a regression model), and less for pure 
data transformation unless there is something that cannot be readily done in SQL. So for clarity, when talking about models in dbt, we should be specific and 
use the words SQL model, or Python model. This will also help avoid confusion with the term **data model**, which is described in more detail below. 
* **Materialization** - Think of materialization as the type of database object your model gets created as. 
The technical materialization types for SQL-based models are **table**, **incremental**, **view**, and **ephemeral**. 
Note that the outputs from Python-based models are always materialized as tables.
* **Table** - This is exactly what you think it is. A model of this materialization is created as a physical table in Databricks. 
The table is recreated each time the model is built, and then fully reloaded with data based on the SQL select statements you have written. 
Behind the scenes, when a table model gets built, dbt is generating the ddl to create the table, as well as the statements to 
insert data to the table. The data inserted to the table comes from the the last select statement in your model. 
This is why all models are required to have at least one select statement. Remember, all models are based on 
select statements because all models are intended to produce a dataset. 
* **Incremental** - This materialization produces a table, but the table is NOT recreated every time the model is built. 
The incremental table is created in Databricks the first time the model is built. There are configuration options you set 
when working with an incremental materialization to control how data is inserted, and updated in the table with subsequent model builds. 
* **View** - This is exactly what you think it is. A model of this materialization is created as a view in Databricks. Like a table model, the view is 
recreated each time the model is built. 
* **Ephemeral** - This materialization produces a CTE in Databricks. In other words, when the model is built, think of it as a temporary object that 
produces a dataset instead of a permanent one. The ephemeral materialization is similar to a view, but it is harder to debug because as mentioned, unlike 
a view it is not permanently persisted to Databricks. For that reason, view materializations are more robust than ephemeral. For our purposes, the vast majority 
of the time if we will use a view materialization instead of an ephemeral materialization. 
* **Run / Build** - Models do not really do anything in dbt until you **run**, or **build** them. Running, or building a model means the SQL statements (or Python code) 
you have written get created as a database object in Databricks. The difference between run, and build is that build also executes tests you have defined on your model. 
* **Tests** - Having tests on models is very important in dbt because it enables us to implement data quality, and integrity checks that are not always 
automatically enforced in modern, cloud data warehouses. Tests can be defined on a model as a whole, or on specific columns in a model. 
We should require certain tests to be on all models. That will be discussed in more detail below. 
* **Data Model** - A data model is how you organize your data. Data modeling has existed for 40+ years. It is not intrinsically tied to dbt. It is a way of structuring 
information into entities, or concepts. There are different techniques for modeling data. There is also a lot of debate about how to approach data modeling in the era 
of the modern data stack (e.g. Databricks and dbt). There is no real, single best approach. We will leverage what I like to call a hybrid-approach, based on what is referred 
to as dimensional modeling. In dimensional modeling, you have tables that are either dimensions, or facts. Dimensions contain the contextual data of your business.
User data is a great example of what we would put into a Member dimension. Facts typically contain numerical output from business processes (measures / metrics), with 
links to the dimensions involved in those processes. Enrollment data is a great example of what we would put into a Conversion fact. A simple metric from the Conversion fact 
is a count of conversions. Dimensions typically relate to multiple facts. Another example of a fact is an Activity fact that records details on exercises a 
Member performs. A simple metric from that fact is a count of exercises. When you draw a picture of a fact, and the dimensions that 
relate to it, it resembles a star, so if you ever hear the phrase star-schema, that is what is being referred to. At the end of the day we should not agonize over whether we should 
call an entity a dimension versus a fact, for multiple reasons. One of those reasons is that another approach we will take in our data modeling is to 
join a fact with its related dimensions, and output the results to what are called information marts, or data marts. The end result of this is a wide, purpose-built, denormalized 
table with all of the data you need to create multiple reports, dashboards, or analyses. The wide table approach is a common approach to use with modern technology because 
we do not have the same limitations, and constraints we had 20+ years ago. For example, disk space is no longer a concern. The key takeaway from this is that 
we aim to have a well-organized data model that is convenient to use, easy to query, and business-friendly. By putting data into wide, denormalized tables, the consumer of the table 
does not have to do any joining of data. 

## General Naming and Field Conventions
A lot of dbt work involves taking some base data — application data, user data, whatever — and cleaning it up for easier use in data work.
As you're cleaning up the data, follow these guidelines:

* Schema, table, and column names should be in **snake_case** (all lower case, using "_" instead of spaces).
* Use names based on the _business_ terminology, rather than the source terminology.
* Each model should have a unique key. This will be explained in more detail later in the section on testing models.
* Name the unique key of a model in the pattern **<object>_id**. For example, the key of the **accounts** model should be **account_id**.
This makes it easier to know what **id** is being referenced in downstream joined models.
* For staging models, fields should be ordered in categories, where identifiers are first and dates/timestamps are at the end.
* Name date/timestamp columns in the pattern **<event>_at**, e.g. **created_at**. UTC is the default timezone.
If you're using a different timezone, indicate it with a suffix, e.g **created_at_pt**, or **created_at_local_tz** if the timezone is row-by-row specific.
* Prefix booleans with **is_** or **has_** or **was_**.
* Format price/revenue fields in decimal currency (e.g. **19.99** for $19.99; many app databases store prices as integers in cents). If non-decimal currency is used, indicate this with suffix, e.g. **price_in_cents**.
* Avoid reserved words as column names. If a source table uses reserved words as column names (e.g. `description`), put it in back-ticks.
* Consistency is key! Use the same field names across models where possible, e.g. a key to the **customers** model 
should be named **customer_id** everywhere it is used rather than **cust_id** in some places, and **customer_id** in other places.

[For reference, please review the official dbt style guide](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)

## Project Structure and Model Naming

Our models are organized into file and folder structures as follows (using a few core, and forecast models as an example):

```
├── analyses
├── macros
└── models
    ├── intermediate
    |   └── core_transformations
    |       └── dimensions
    |           ├── _int_ct_dimensions_models.yml
    |           ├── int_accounts.sql
    |       └── facts
    |           ├── _int_ct_facts_models.yml
    |           ├── int_marketing_touchpoints.sql
    |       ├── _int_ct_models.yml
    |       ├── int_customers_joined_to_indications.sql
    |   └── forecasting_transformations
    |       └── multipliers
    |           ├── _int_ft_multipliers_models.yml
    |           ├── int_trailing_multipliers_aggregated_.sql  
    ├── marts
    |   └── dimensions
    |       ├── _dim_dimensions_models.yml
    |       ├── dim_accounts.sql
    |   └── facts
    |       └── core
    |           ├── _fct_core_models.yml
    |           ├── fct_deployments.sql
    |       └── forecasting
    |           ├── _fct_forecasting_models.yml
    |           ├── fct_marketing_touchpoints_consolidated.sql
    |       └── other
    |           └── rpt_marts
    |               ├── _rpt_marts_models.yml
    |               ├── rpt_marketing_touchpoints_consolidated.sql
    |           └── rpt_marts_legacy
    |               ├── _rpt_marts_legacy_models.yml
    |               ├── rpt_legacy_marketing_touchpoints_historical.sql
    ├── staging
    |   └── sfdc_rollups
    |       ├── _sfdc__models.yml
    |       ├── _sfdc__sources.yml
    |       ├── stg_sfdc_rollups__marketing_touchpoints.sql
├── seeds
├── snapshots
├── tests
├── README.md
├── dbt_project.yml
├── packages.yml
└── style_guide.md
```
### WHAT FOLLOWS BELOW IS FROM AN OLD PROJECT, NOT RELATED TO HINGE HEALTH. 
### PLEASE SKIP ALL SECTIONS BELOW UNTIL THEY ARE UPDATED. 


### 1. Staging Models

* Staging models are used to select data from a source, and prepare it for use by downstream models and tranformations. The staging model
is where we begin to use names that are more business-oriented, and less source-oriented. In a staging model we might cast columns to appropriate
data types, and code case statements or other basic calculations that will be used downstream.
* One important thing to keep in mind about staging models is that they typically do not join data together from multiple sources.
* Staging models are named and organized into folders and sub-folders corresponding to the data source, schema, and business concept they represent.
* The naming convention to follow is `stg_<data_source_schema_name>__<source_table_name>`, e.g. `stg_sfdc_rollups__accounts`.
* _NB: two underscores separate schema name from table name._
* Dependencies: Staging models should be built from exactly one source.
* Model materialization: view
* Primary folder: staging\data_source_schema_name
* Configuration files: `_<data_source>__models.yml`, `_<data_source>__sources.yml`

### 2. Intermediate Models

* Intermediate models are named and organized into folders and sub-folders according to subject area or business domain.
* Model materialization: view
* Primary folder: intermediate\subject_area\

* _NB: There are two types of business rule models, each with a specific nomenclature and file structure:_

#### Type 1: "Base" Business Rules

* Type-1 `base` business rule models prepare staging tables for later use. Base models rename fields to business-friendly terms, do general data cleanup, coalesce nulls, do basic field calculations, or other prep work. 
* Base business rules are named in the format `br_<source_schema_name>_<source_table_name>`, e.g. `br_stripe_mobile_balance_transactions`. (Note only one underscore between schema name and table name.) Base business rules also live in their own subfolder within the business rules folder.
* Dependencies: In most cases, `base` business rule models will be built from one staging model.

#### Type 2: "Concept" Business Rules

* Type-2 business rule models represent business _concepts_. They go a step further from base br models, and begin to shape, transform, and alter data, _or_ join together multiple models to implement more complex rules. 
* Business concept models are named according to the primary business concept they represent, in the format `br_<business_concept_name>`, e.g. `br_claim`. (_NB: the business concept name is singular, not plural._)
* Dependencies: Business concept models can built from one or more other business rule models, or staging models.

### 4. Information Mart Models

* Information mart models are named and organized into folders and sub-folders corresponding to the business concepts they represent, as well as the temporal nature of their data.
* There are three types of information mart models, described in more detail below.

#### A. Current-Value Information Marts

* Information marts that provide current-value (i.e. "present-tense") data should be in the format `im_<concept_name>`. For example, `im_claim` is a claim information mart containing only current-value data for all claim attributes.
* Current-value information marts are the most basic and should be implemented first.
* Dependencies: Current-value information marts are typically dependent on one or more business rule models, and may depend on other information marts. 
    * We might base an information mart on another information mart according to how much data processing we need to create an output. If multiple information marts rely on a resource-intensive calculation, for example, then it's probably best to do the calculation _once_ in one information mart. From there, any other information marts that need the same calculation can be built leveraging the calculation that was done in the first information mart.
* Model materialization: table (full reload)
* Primary folder: 4_info_mart\concept_name\current

#### B. Historical Data Information Marts

* Information marts that provide historical data should be in the format `im_<concept_name>_<hist>`; for example, `im_claim_hist` is a claim information mart containing a _history_ of how claims changed over time. We can track historical data in two ways:
    * Option 1: incrementally insert new or changed records using a primary key and a hash-diff comparison.
    * Option 2: Alternately, use the dbt `snapshot` functionality if we need something that more closely resembles a [type-2 slowly changing dimension](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row).
* Dependencies: We can build historical-value information marts directly from current-value information marts (hence why we should create current-value information marts first).
* Model materialization: incremental (table)
* Primary folder: 4_info_mart\concept_name\historical

#### C. Point in Time (PIT) Data Information Marts

* Point-in-time information marts provide a periodic image of what data looked like at a certain point in time. Name them in the format `im_<concept_name>_pit`. For example,  `im_claim_pit` is a claim information mart containing a record of all claims _as of_ a specified periodic interval (e.g. monthly, quarterly, etc.)
* _NB1: The difference between a `_hist` and `_pit` mart is that a `_hist` mart tracks changes only whereas a `_pit` mart contains a record for each entity regardless of any changes that have or have not occurred since the prior `pit` load._
    * `_hist` example: Let's say `im_claim_hist` is being loaded several times a day, and that we have a claim in the mart, `claim_id 24601`. Every time we load the mart table, dbt creates a new record _only_ if `claim_id 24601` has not already been loaded, or if a tracked attribute in `claim_id 24601` has changed since the prior load. 
    * `_pit` example: Let's say `im_claim_pit` is being loaded monthly, and that we still have `claim_id 24601`. With each monthly load, a new record is inserted for `claim_id 24601` each time we take the monthly `pit` image, even if nothing in `claim_id 24601` has changed since the prior load. This is because the purpose of a `_pit` mart is to summarize all activity (or non-activity) during or at the end of a time span, rather than capture changes. This is akin to a type of fact table in dimensional modeling known as a *periodic snapshot*.
    * As alluded to in the example, all reporting entities will appear in each `_pit` image, even if there's no activity since the prior load.
* Dependencies: Point-in-time information marts can be built directly from current-value information marts, hence the need to create current value information marts first.
* Model materialization: incremental (table)
* Primary folder: 4_info_mart\concept_name\point_in_time

### 5. Metrics Rule Models

* Metrics rule models are named and organized into folders and sub-folders corresponding to business concepts and the specific metrics or report output they represent.
* These models should be in the format `mr_[metrics_report_name]`, e.g. `mr_balance_transaction` is a specialized instance of balance transaction metrics, containing a subset of all balance transaction types.
* Dependencies: Metrics rule models can be built from one or more information marts.
* Model materialization: view
* Primary folder: 5_metrics_rule/concept_name

### 6. Metrics Mart Models

* Metrics mart models are named and organized into folders and sub-folders corresponding to business concepts, the specific metrics or report output they represent, as well as the temporal nature of their data.
* There are three types of metrics mart models and they mirror the three types of information mart models: current, historical, and point_in_time.
* Dependencies: Metrics mart models can be built from one or more metrics rule models or other metrics mart models.
* Model materialization: same as for the three types of information mart models (table, incremental, incremental).
* Primary folder: 6_metrics_mart/concept_name

## "Which Model Do I Use?": A Helpful Guide

### User-facing Mart Views

To recap, there are two mart layers: _information_ marts and _metrics_ marts.
* It's good practice to only make information marts and metrics marts accessible to users through **views**, because then it's easier to make modifications to any underlying physical artifacts (i.e. tables) the views are based on, without adversely impacting who or what tool may be accessing the data. In other words, the views essentially function as interfaces to the data, with the details hidden behind the facade a user or tool is presented with. 
* The view names should mirror the format of the original information marts or metrics marts they are selecting data from, but prefixed with a `v` to distinguish them from the original tables. For example, `vim_claim` is a user-facing information mart view based on the original `im_claim` table, and `vmm_balance_transaction` is a user-facing metrics mart view based on the physical `mm_balance_transaction` table.
* Dependencies: Information mart views are typically based on a single, physical information mart. Metrics mart views are typically based on a single, physical metrics mart.
* Model materialization: view
* Primary folder: 4_info_mart\concept_name, and 6_metrics_mart\concept_name

### Business Rule and Information Mart Models vs. Metrics-Level Models

Business rule and information mart models differ from their metrics-level counterparts (metrics rules and metrics marts, respectively) in their sense of scope and breadth.
* Think of business rule models and information marts as _solid, ready-to-go building blocks_. They produce data covering a broad business concept. They'll clean up the data from the original source tables, and may join together some different tables to create a single convenient table that's clean and ready to go. 
* Metrics rule models and metrics marts, on the other hand, provide _narrow, purpose-built slices of data and metrics_ representing a subset of, or specialized version of, a broader business concept. If you need data that starts with "only clients who have done these three things..." or "only sessions with this important characteristic," then you should consider building that as a metrics-level model.

#### Example: Stripe Balance Transactions

In Stripe data, "balance transactions" are the base unit of cash transactions. They form the basis of a _lot_ of Stripe data analysis.

* The Stripe `br_balance_transaction` model is the source for the `im_balance_transaction` information mart. Together, the business concept model and the information mart model provide the rules and output for a general, business-level concept: the Stripe balance transaction. If you're starting a new analysis from scratch, you'll probably want to start with the information mart.
* The `mr_balance_transaction` metrics rule model and `mm_balance_transaction` metrics mart model leverage the balance_transaction information mart (and others) to provide a specific "cash-basis" view based on a subset of transaction types, which we use specifically for a few important financial reports. They _don't_ necessarily include all the data from the original information mart, but they _do_ serve the reports we need.

## CTE Guidelines

- All `{{ ref('...') }}` statements should be placed in CTEs at the top of the file.
- Where performance permits, CTEs should perform a single, logical unit of work.
- CTE names should be as verbose as needed to convey what they do.
- Generally, try to aggregate and join in separate CTEs, rather than aggregating and joining all at once. This is more performant and more modular.
- Comment the heck out of CTEs with confusing or noteable logic.
- CTEs that are duplicated across models should be pulled out into their own models — don't repeat yourself!
- Create a `final` or similar CTE that you select from as your last line of code. This makes it easier to debug code within a model (without having to comment out code!)
- CTEs should be formatted like this:

``` sql
with

events as (

    ...

),

-- CTE comments go here
filtered_events as (

    ...

)

select * 
from filtered_events
where true
```

## SQL Style Guidelines

- Use trailing commas
- Always add `where true` condition, and add additional filters below with indents. 
- Indents should be four spaces (except for predicates, which should line up with the `where` keyword).
- No need to indent or work on the next line if you're only selecting from one table. For example:
``` sql
-- No need to indent one-line clauses, even if other clauses are multi-line:
select cattos.*
from pet_schema.cats as cattos
where true
    and cattos.name = "Sebastian"

-- Break out multi-line clauses:
select
    doggo_name,
    treato_id
from
    pet_schema.dogs as doggos
    inner join snacks.treatos
        on doggos.fav_treato_id = treatos.id
where true
    and doggos.is_good = 1
```

- In general, and especially in upstream models like staging and business rule models, organize fields by the following categories, in the following order, and comment them as such:
    1. primary key(s) (labeled `pks`)
    2. foreign keys (labeled `fks`)
    3. misc
    4. dates
    5. metrics
    6. calculated columns (including case statements)
    7. metadata

Here's an abbreviated example from the `session_report` information mart:
``` sql
 -- pks
 session_report_id,
 
 -- fks
 case_id,
 room_id,
 session_modality_id,

 -- misc
 report_position,
 report_name,
 is_automatic_submission,

 -- dates
 completed_at,
 started_at,
 ended_at,
 reopened_at,

 -- metrics
 max_cost_of_service,

  -- calculated columns
 case 
     when completed_at is not null
     then True
     else False
 end is_complete,

 -- metadata
 {{ select_dms_metadata_cols('session_report') }},
 {{ select_dbt_metadata_cols('session_report') }}
```

- Lines of SQL should be 80 characters or less.
- Lowercase all field names, function names, and sql keywords.
- Always use the `as` keyword when aliasing a field or table. This improves readability.
- In the select, state fields before aggregates / window functions.
- Execute aggregations as early as possible, before joining to another table.
- Ordering and grouping by a number (eg. group by 1, 2) is preferred over listing the column names (see [this rant](https://blog.getdbt.com/write-better-sql-a-defense-of-group-by-1/) for why).
- Prefer `union all` to `union` [*](http://docs.aws.amazon.com/redshift/latest/dg/c_example_unionall_query.html)
- Do not use single-letter aliases for tables. If a table has a single-word name (e.g. `plan` or `charges`), keep the table name or use a shortened version (e.g. `chg` for `charges`).
- Be careful about abbreviating two-word tables with common abbreviations; `pt` could be `payment_transactions`, `payment_type`, `private_talks`, etc. In these scenarios, refer to the Confluence table documentation for suggested abbreviations (e.g. we customarily shorten `payment_transactions` to `tx`).
- If joining two or more tables, _always_ prefix your column names with the table alias. If only selecting from one table, you don't need prefixes, but they're encouraged.
- Be explicit about your join (i.e. write `inner join` instead of `join`). `left joins` are normally the most useful, `right joins` often indicate that you should change which table you select `from` and which one you `join` to.

- *DO NOT OPTIMIZE FOR A SMALLER NUMBER OF LINES OF CODE. NEW LINES ARE CHEAP, BRAIN TIME IS EXPENSIVE*

### Example SQL
```sql
with

my_data as (

    select *
    from {{ ref('my_data') }}
    where true

),

some_cte as (

    select * 
    from {{ ref('some_cte') }}
    where true

),

some_cte_agg as (

    select
        id,
        sum(field_4) as total_field_4,
        max(field_5) as max_field_5
    from some_cte
    group by 1

),

final as (

    select distinct
        my_data.field_1,
        my_data.field_2,
        my_data.field_3,

        -- use line breaks to visually separate calculations into blocks
        case
            when my_data.cancellation_date is null
                and my_data.expiration_date is not null
                then expiration_date
            when my_data.cancellation_date is null
                then my_data.start_date + 7
            else my_data.cancellation_date
        end as cancellation_date,

        some_cte_agg.total_field_4,
        some_cte_agg.max_field_5

    from
        my_data
        left join some_cte_agg  
            on my_data.id = some_cte_agg.id
    where true 
        and my_data.field_1 = 'abc'
        and (
            my_data.field_2 = 'def' or
            my_data.field_2 = 'ghi'
        )
    having count(*) > 1

)

select *
from final

```

- Your join should list the "left" table first (i.e. the table you are selecting `from`):
```sql
select
    trips.*,
    drivers.rating as driver_rating,
    riders.rating as rider_rating
from
    trips
    left join users as drivers
        on trips.driver_id = drivers.user_id
    left join users as riders
        on trips.rider_id = riders.user_id

```
