-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databases Mapping
-- MAGIC  Are databases present in both EDITH and CEX ?<br>
-- MAGIC  Are all databases having the same value for their mapping:
-- MAGIC * Is Stale (Last refresh date >= 90 days)
-- MAGIC * Is Included in Gold
-- MAGIC * Database Exists

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View - DataverseComparison

-- COMMAND ----------

create or replace table mdl_europe_anz_dev.silver_edith_nielsen.DataverseComparison as (
  -- Databases Id in Edith Data 
  with EdithDatabase as (
      select
        DatabaseId as EdithDatabaseId,
        CexDatabaseId, -- Edith DatabaseName mapped to Cex Name
        IsStaleDatabase as EdithIsStale,
        IncludeGold as EdithIncludeGold,
        DatabaseExist::boolean as EdithDatabaseExist
      from hive_metastore.silver_mdl_dataverse.edf_edith_nielsen_database
      where IsStaleDatabase = false
  )
  , CexDatabase as (
    select
      DatabaseId as CexDatabaseId,
      (DaysSinceRefreshDatabase >= 90)::boolean as CexIsStale,
      (IncludeGold = 'Y')::boolean as CexIncludeGold,
      (DatabaseExist = 'Y')::boolean as CexDatabaseExist
      from hive_metastore.silver_mdl_dataverse.edf_bdl_nielsen_cex_database
    where DatabaseExist = 'Y'
  )
  , DatabaseJoin as (
    Select
      coalesce(cex.CexDatabaseId, edith.CexDatabaseId) as CexDatabaseId,
      CexIsStale,
      CexIncludeGold,
      CexDatabaseExist,
      EdithDatabaseId,
      EdithIsStale,
      EdithIncludeGold,
      EdithDatabaseExist,
      (cex.CexDatabaseId = edith.CexDatabaseId) as IsDatabaseMatch,
      (cex.CexDatabaseId is not null) as InCex,
      (edith.CexDatabaseId is not null) as InEdith,
      struct(InCex, inEdith) as DatabaseComparison,
      (CexIsStale = EdithIsStale) as IsStaleMatch,
      struct(CexIsStale, EdithIsStale) as IsStaleComparison,
      (CexIncludeGold = EdithIncludeGold) as IsIncludeGoldMatch,
      struct(CexIncludeGold, EdithIncludeGold) as IncludeGoldComparison
    from
      CexDatabase as cex
    full outer join EdithDatabase as edith 
      using (CexDatabaseId)
  )
  , DatabaseComparison as (
  select
    'Mapping' as Dimension,
    case
      when IsDatabaseMatch = true and IsStaleMatch = true and IsIncludeGoldMatch = true then "1 - In Both - Mappings Align"
      when IsDatabaseMatch = true and (IsStaleMatch = false or IsIncludeGoldMatch = false) then "2 - In Both - Mappings Differ"
      when InCEx = false then "3 - Not in CEX"
      when InEdith = false then "4 - Not in EDITH"
      else "5 - Unknown"
    end as Status,
    Ifnull((IsDatabaseMatch = true and IsStaleMatch = true and IsIncludeGoldMatch = true), false) as IsPassing,
    if(IsDatabaseMatch, 'Pass', 'Fail') as DataversePass,
    if(IsPassing, 'Pass', 'Fail') as MappingPass,
    *
  from DatabaseJoin
)
  Select * from DatabaseComparison
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  Dataverse Comparison Results

-- COMMAND ----------

Select
  status,
  count(CexDatabaseId) as DatabaseCount,
  array_agg(CexDatabaseId) as DatabaseList
from mdl_europe_anz_dev.silver_edith_nielsen.DataverseComparison
group by 1
order by 1

-- COMMAND ----------

Select 
  Dimension, 
  Status,
  MappingPass,
  count(*) as Count,
  array_agg(struct(CexDatabaseId, EdithDatabaseid, DatabaseComparison, IsStaleComparison,  IncludeGoldComparison)) as Comparison
from mdl_europe_anz_dev.silver_edith_nielsen.DataverseComparison
group by 1, 2, 3
order by Status

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2 - In Both - Mappings Differ

-- COMMAND ----------

-- Details for '2 - In Both - Mappings Differ'
-- To refresh the query cell 3 has to be ran first
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872750272
--------------------------------------------------
select
  Dimension,
  CEXDatabaseId, 
  EdithDatabaseId,
  struct(CexIsStale, EdithIsStale) as IsStaleComparison,
  struct(CexIncludeGold, EdithIncludeGold) as IncludeGoldComparison
from mdl_europe_anz_dev.silver_edith_nielsen.DataverseComparison 
where Status = "2 - In Both - Mappings Differ"


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3 - Not in CEX

-- COMMAND ----------

-- Details for '3 - Not in CEX'
-- To refresh the query cell 3 has to be ran first
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872750272
--------------------------------------------------
select
 *
from mdl_europe_anz_dev.silver_edith_nielsen.DataverseComparison 
where Status = "3 - Not in CEX"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4 - Not in EDITH

-- COMMAND ----------

-- Details for '4 - Not in EDITH'
-- To refresh the query cell 3 has to be ran first
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872750272
--------------------------------------------------
select
 *
from mdl_europe_anz_dev.silver_edith_nielsen.DataverseComparison 
where Status = "4 - Not in EDITH"
group by all;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5 - Unknown

-- COMMAND ----------

-- Details for '5 - Unknown'
-- To refresh the query cell 3 has to be ran first
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872750272
--------------------------------------------------
select
 *
from mdl_europe_anz_dev.silver_edith_nielsen.DataverseComparison 
where Status = "5 - Unknown"
group by all;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Periods
-- MAGIC Are Databases passing Mapping comparison present in EDITH and CEX Fact?<br>
-- MAGIC Are Databases having the same periods:
-- MAGIC * Start Date
-- MAGIC * End Date
-- MAGIC * Periods between Start and End Dates
-- MAGIC * Number of periods in Fact Table
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View - PeriodComparison

-- COMMAND ----------

Create or replace table mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison as (
  with DataversePass as (
    Select 
      CexDatabaseId,
      EdithDatabaseId,
      CexIsStale,
      CexIncludeGold,
      CexDatabaseExist,
      EdithDatabaseId,
      EdithIsStale,
      EdithIncludeGold,
      EdithDatabaseExist
    From mdl_europe_anz_dev.silver_edith_nielsen.DataverseComparison
    where MappingPass = 'Pass'
  )
  , CexFactSilver as (
    Select distinct
      DatabaseId,
      WeekEnd
    from mdl_europe_anz.silver_bdl_nielsen_cex.fact as f
    left join mdl_europe_anz.silver_bdl_nielsen_cex.period using (YYYYWW)
  )
  , CexPeriod as (
    Select 
      DatabaseId as CexDatabaseId,
      min(WeekEnd) as CexFirstDate,
      max(WeekEnd) as CexLastDate,
      ((datediff(max(WeekEnd), min(WeekEnd)) / 7) + 1)::int as CexPeriodDiff,
      count(distinct WeekEnd)::int as CexPeriodCount
    from CexFactSilver
    group by 1
  )
  , EdithPeriod as (
    Select
      DatabaseId as EdithDatabaseId,
      -- CexDatabaseId,
      -- EdithDatabaseId,
      min(DateId) as EdithFirstDate,
      max(DateId) as EdithLastDate,
      ((datediff(max(DateId), min(DateId)) / 7) + 1)::int as EdithPeriodDiff,
      count(distinct DateId)::int as EdithPeriodCount
      -- (EdithDiffPeriods = EdithPeriodCount) as EdithHasNoMissingPeriods,
      -- (EdithDiffPeriods >= 156) as EdithHas3Yearsdata
    from
      mdl_europe_anz_dev.silver_edith_nielsen.fact
    group by 1  
  )
  , JoinPeriod as (
    select 
      coalesce(db.CexDatabaseId, cex.CexDatabaseId) as CexDatabaseId,
      coalesce(db.EdithDatabaseId, edith.EdithDatabaseId) as EdithDatabaseId,
      (cex.CexDatabaseId is not null) as InCex,
      (edith.EdithDatabaseId is not null) as InEdith,
      (InCex = true and InEdith = true) as IsDatabaseMatch,
      (CexFirstDate = EdithFirstDate) as IsFirstDateMatch,
      struct(IsFirstDateMatch, CexFirstDate, EdithFirstDate) as CheckFirstDates,
      (CexLastDate = EdithLastDate) as IsLastDateMatch,
      struct(IsLastDateMatch, CexLastDate, EdithlastDate) as CheckLastDates,
      (IsFirstDateMatch = true and IsLastDateMatch = true) as IsDateRangeMatch,
      (CexPeriodDiff = EdithPeriodDiff) as IsPeriodDiffMatch,
      (CexPeriodCount = EdithPeriodCount) as IsPeriodCountMatch,
      struct(IsPeriodDiffMatch, CexPeriodDiff, EdithPeriodDiff) as CheckPeriodDiff,
      struct(IsPeriodCountMatch, CexPeriodCount, EdithPeriodCount) as CheckPeriodCount,
      (IsPeriodDiffMatch = true and IsPeriodCountMatch = true) as IsPeriodMatch,
      if(CexFirstDate > EdithFirstDate, CexFirstDate, EdithFirstDate) as OverlapFirstDate,
      if(CexLastDate < EdithLastDate, CexLastDate, EdithLastDate) as OverlapLastDate,
      datediff(OverlapLastDate, OverlapFirstDate) / 7 + 1 as OverlapPeriodDiff,
      abs(datediff(CexFirstDate, EdithFirstDate)) / 7 as RefreshDelay,
      OverlapPeriodDiff = CexPeriodDiff - RefreshDelay as IsOverlapMatch,
      case
        when CexFirstDate > EdithFirstDate then "Cex Late"
        when CexFirstDate < EdithFirstDate then "Edith Late"
        else "Both Aligned" 
      end as RefreshStatus,
      IF(CexFirstDate < EdithFirstDate, CexFirstDate, EdithFirstDate) as FirstDate,
      IF(CexFirstDate < EdithFirstDate, "CEX", "EDITH") as FirstDateDatabase,  
      IF(CexLastDate > EdithLastDate, CexLastDate, EdithLastDate) as LastDate,
      IF(CexLastDate > EdithLastDate, "CEX", "EDITH") as LastDateDatabase,      
      CexIsStale,
      CexIncludeGold,
      CexDatabaseExist,
      EdithIsStale,
      EdithIncludeGold,
      EdithDatabaseExist,
      CexFirstDate,
      CexLastDate,
      CexPeriodDiff,
      CexPeriodCount,
      EdithFirstDate,
      EdithLastDate,
      EdithPeriodDiff,
      EdithPeriodCount
    from DataversePass as db
    left join EdithPeriod as edith on db.EdithDatabaseId = edith.EdithDatabaseId
    left join CexPeriod  as cex on db.CexDatabaseId = cex.CexDatabaseId 
  )
  , PeriodComparison as (
    Select
      'Period' as Dimension,
      case
        when IsDatabaseMatch = true and IsDateRangeMatch = true and IsPeriodMatch = true then "1 - In Both - Periods Align"
        when IsDatabaseMatch = true and IsDateRangeMatch = false and IsPeriodMatch = true and IsOverlapMatch = true then "2 - In Both - Refresh Dates Differ"
        when IsDatabaseMatch = true and (IsDateRangeMatch = false or IsPeriodMatch = false) then "3 - In Both - Periods Differ"
        when InCex = false and InEdith = true then "4 - No fact in CEX"
        when InCex = true and InEdith = false then "5 - No fact in EDITH"
        else "6 - Unknown"
      end as Status,
      IF(IsDatabaseMatch = true and IsDateRangeMatch = true and IsPeriodMatch = true, 'Pass', 'Fail' )as PeriodPass,
      *
    from JoinPeriod
  )
  select * from PeriodComparison
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Periods Comparison Results

-- COMMAND ----------

Select
  Status,
  count(CexDatabaseId) as DatabaseCount,
  array_agg(CexDatabaseId) as DatabaseList
from mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison
group by all
order by 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2 - In Both - Refresh Dates Differ

-- COMMAND ----------

-- Details for '2 - In Both - Periods Differ'
-- To refresh the query following cells have to be ran first
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872750272
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872752613
--------------------------------------------------
select
  -- Dimension, 
  -- Status,
  CexDatabaseId,
  EdithDatabaseId,
  struct(
    RefreshStatus,
    RefreshDelay,
    FirstDate,
    FirstDateDatabase,
    OverlapFirstDate,
    OverlapLastDate,
    LastDate,
    LastDateDatabase,
    OverlapPeriodDiff
  ) as StatusReason
from mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison 
where Status = "2 - In Both - Refresh Dates Differ" ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3 - In Both - Periods Differ

-- COMMAND ----------

-- Details for '2 - In Both - Periods Differ'
-- To refresh the query following cells have to be ran first
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872750272
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872752613
--------------------------------------------------
select
  -- Dimension, 
  -- Status,
  CexDatabaseId,
  EdithDatabaseId,
  CheckFirstDates,
  CheckLastDates,
  CheckPeriodDiff,
  CheckPeriodCount
from mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison 
where Status = "3 - In Both - Periods Differ";

-- COMMAND ----------

Select 
  DatabaseId,
  sum(if(YYYYWW is null, 1, 0)) as NullRecords,
  count(*) as Records
from mdl_europe_anz.silver_bdl_nielsen_cex.fact
where true
  and DatabaseId in ('CEX_GR_BLEACH_W', 'CEX_IE_SKINCLEANSING_W')
group by 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4 - No fact in CEX

-- COMMAND ----------

-- Details for '3 - Not in CEX'
-- To refresh the query following cells have to be ran first
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872750272
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872752613
--------------------------------------------------
select
 *
from mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison 
where Status = "4 - No fact in CEX"
group by all;

-- COMMAND ----------

Select 
  DatabaseId, 
  count(distinct YYYYWW) as PeriodsCount,
  Count(*) as RecordsCount
from mdl_europe_anz.silver_bdl_nielsen_cex.fact
where DatabaseId in (
    Select CexDatabaseId 
    from mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison
    where Status = "4 - No fact in CEX")
group by 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5 - No fact in EDITH

-- COMMAND ----------

-- Details for '3 - Not in EDITH'
-- To refresh the query following cells have to be ran first
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872750272
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872752613
--------------------------------------------------
select
 *
from mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison 
where Status = "5 - No fact in EDITH"
group by all;

-- COMMAND ----------

with EdithDatabasesFailing as (
  Select distinct 
    CexDatabaseId,
    EdithDatabaseId 
  from mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison
  where Status = "5 - No fact in EDITH"
)
Select 
  e.CexDatabaseId,
  e.EdithDatabaseId,
  dbc.DaysSinceRefreshDatabase >= 90 as CexIsStale, 
  dbc.IncludeGold as CexIncludeGold,
  dbe.IsStaleDatabase as EdithIsStale,
  dbe.IncludeGold as EdithIncludeGold
  -- count(distinct cex.YYYYWW) as CexPeriodsCount,
  -- count(cex.YYYYWW) as CexRecordsCount,
  -- count(distinct edith.YYYYWW) as EdithPeriodsCount,
  -- count(edith.YYYYWW) as EdithRecordsCount
from EdithDatabasesFailing as e
  -- left join mdl_europe_anz_dev.silver_edith_nielsen.fact as edith
  --   on e.EdithDatabaseId = edith.DatabaseId
  left join hive_metastore.silver_mdl_dataverse.edf_edith_nielsen_database as dbe
    on e.EdithDatabaseId = dbe.DatabaseId
  -- left join mdl_europe_anz.silver_bdl_nielsen_cex.fact as cex
  --   on e.CexDatabaseId = cex.DatabaseId
  left join hive_metastore.silver_mdl_dataverse.edf_bdl_nielsen_cex_database as dbc
    on e.CexDatabaseId = dbc.DatabaseId
group by 1, 2, 3, 4, 5, 6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6 - Unknown

-- COMMAND ----------

-- Details for '3 - Not in EDITH'
-- To refresh the query following cells have to be ran first
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872750272
-- https://adb-4114520672978928.8.azuredatabricks.net/?o=4114520672978928#notebook/1423635611882633/command/1326968872752613
--------------------------------------------------
select
 *
from mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison 
where Status = "6 - Unknown"
group by all;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Markets
-- MAGIC Are Databases having the same market tags in CEX and EDITH?<br>
-- MAGIC Checks, for databases that passed the mapping test, if their Market Tag Codes match between EDITH and CEX.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View - MarketComparison

-- COMMAND ----------

create or replace table mdl_europe_anz_dev.silver_edith_nielsen.MarketComparison as (
  with DataversePass as (
    Select 
      CexDatabaseId,
      EdithDatabaseId,
      Status as PeriodStatus
    From mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison
    where Status in (
      '1 - In Both - Periods Align',
      '2 - In Both - Refresh Dates Differ')
  )
  , Cex_Market as (
    select distinct 
      CexDatabaseId,
      EdithDatabaseId,
      PeriodStatus,
      MarketTagCode as CexMarketTagCode
    from
      DataversePass
      -- left join mdl_europe_anz.silver_bdl_nielsen_cex.Market
      left join mdl_europe_anz.silver_bdl_nielsen_cex.Market
        on CexDatabaseId = DatabaseId
  )
  -- Market Tags by Database on CEX
  , Edith_Market as (
    select distinct
      CexDatabaseId,
      EdithDatabaseId,
      PeriodStatus,
      MarketTagCode as EdithMarketTagCode
    from
      DataversePass
      left join mdl_europe_anz_dev.silver_edith_nielsen.Market
        on EdithDatabaseId = DatabaseId
    where IsCustomMarket = false
  )
  , JoinMarket as (
    Select
        coalesce(cex.CexDatabaseId, edith.CexDatabaseId) as CexDatabaseId,
        coalesce(cex.EdithDatabaseId, edith.EdithDatabaseId) as EdithDatabaseId,
        coalesce(cex.PeriodStatus, edith.PeriodStatus) as PeriodStatus,
        coalesce(CexMarketTagCode, EdithMarketTagCode) as MarketTagCode,
        (cex.CexDatabaseId is not null) as InCex,
        (edith.EdithDatabaseId is not null) as InEdith,
        (InCex = true and InEdith = true) as IsDatabaseMatch,
        (CexMarketTagCode = EdithMarketTagCode) as IsMarketMatch,
        CexMarketTagCode,
        EdithMarketTagCode
    from
      Cex_Market as cex 
      full outer join Edith_Market as edith 
        on cex.CexDatabaseId = edith.CexDatabaseId
        and cex.CexMarketTagCode = edith.EdithMarketTagCode

  )
  , MarketComparison as (
    select
      'Market' as Dimenstion,
      case 
        when IsDatabaseMatch = true and IsMarketMatch = true then "1 - In Both - Markets Align"
        when IsDatabaseMatch = true and IsMarketMatch = False then "2 - In Both - Markets Differ"
        when CexMarketTagCode is null then "3 - Not in CEX"
        when EdithMarketTagCode is null then "4 - Not in EDITH"
        else "5 - Unknown"
      End as Status,
      if(IsDatabaseMatch = true and IsMarketMatch = true, 'Pass', 'Fail') as MarketPass,
      *
    from JoinMarket

  )
  Select * from MarketComparison
  order by 1
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Markets Comparison Results

-- COMMAND ----------

select
  Status,
  PeriodStatus,
  count(distinct CEXDatabaseId) as DatabaseCount,
  format_number(count(distinct MarketTagCode), "#,###") as MarketCount,  
  array_agg(distinct CEXDatabaseId) as Databases
from mdl_europe_anz_dev.silver_edith_nielsen.MarketComparison
group by all
order by 1


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2 - In Both - Markets Differ

-- COMMAND ----------

Select
  *
from mdl_europe_anz_dev.silver_edith_nielsen.MarketComparison
where Status = '2 - In Both - Markets Differ'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3 - Not in CEX

-- COMMAND ----------

Select
  *
from mdl_europe_anz_dev.silver_edith_nielsen.MarketComparison
where Status = '3 - Not in CEX'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4 - Not in EDITH

-- COMMAND ----------

Select
  *
from mdl_europe_anz_dev.silver_edith_nielsen.MarketComparison
where Status = '4 - Not in EDITH'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Products
-- MAGIC Are Databases having the same products tags in CEX and EDITH?<br>
-- MAGIC Checks, for databases that passed the mapping test, if their products tag match between EDITH and CEX.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View - ProductComparison

-- COMMAND ----------

create or replace table mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison as (
  with DatabasePass as (
    Select 
      CexDatabaseId,
      EdithDatabaseId,
      MarketTagCode,
      PeriodStatus,
      Status as MarketStatus 
    From mdl_europe_anz_dev.silver_edith_nielsen.MarketComparison
    where MarketPass = 'Pass'
  )
  , Cex_Product as (
    select distinct 
      CexDatabaseId,
      EdithDatabaseId,
      PeriodStatus,
      MarketStatus,
      ProductTagCode as CexProductTagCode,
      AttributeGroupHarmonized.Format as CexHierarchyFormat,
      AttributeGroupHarmonized.Segment as CexHierarchySegment,
      HierarchyNumber as CexHierarchyNumber
    from
      DatabasePass
      -- left join mdl_europe_anz.silver_bdl_nielsen_cex.product
      left join mdl_europe_anz.silver_bdl_nielsen_cex.product
        on CexDatabaseId = DatabaseId
    where 
      HierarchyNumber = 1
      and IsMaxLevel
  )
  -- Product Tags by Database on CEX
  , Edith_Product as (
    select distinct
      CexDatabaseId,
      EdithDatabaseId,
      PeriodStatus,
      MarketStatus,
      ProductTagCode as EdithProductTagCode,
      HierarchyExternalGroup.Format as EdithHierarchyFormat,
      HierarchyExternalGroup.Segment as EdithHierarchySegment
    from
      DatabasePass
      left join mdl_europe_anz_dev.silver_edith_nielsen.product
        on EdithDatabaseId = DatabaseId
    where IsUnmaskingRow = false
  )
  , JoinProduct as (
    Select
        coalesce(cex.CexDatabaseId, edith.CexDatabaseId) as CexDatabaseId,
        coalesce(cex.EdithDatabaseId, edith.EdithDatabaseId) as EdithDatabaseId,
        coalesce(CexProductTagCode, EdithProductTagCode) as ProductTagCode,
        coalesce(cex.PeriodStatus, edith.PeriodStatus) as PeriodStatus,
        coalesce(cex.MarketStatus, edith.MarketStatus) as MarketStatus,
        (cex.CexDatabaseId is not null) as InCex,
        (edith.EdithDatabaseId is not null) as InEdith,
        (InCex = true and InEdith = true) as IsDatabaseMatch,
        (CexProductTagCode = EdithProductTagCode) as IsProductMatch,
        (EdithHierarchyFormat = CexHierarchyFormat) as IsFormatMatch,
        (EdithHierarchySegment = CexHierarchySegment) as isSegmentMatch,
        CexProductTagCode,
        CexHierarchyFormat,
        CexHierarchySegment,
        CexHierarchyNumber,
        EdithProductTagCode,
        EdithHierarchyFormat,
        EdithHierarchySegment

    from
      Cex_Product as cex 
      full outer join Edith_Product as edith 
        on cex.CexDatabaseId = edith.CexDatabaseId
        and cex.CexProductTagCode = edith.EdithProductTagCode

  )
  , ProductComparison as (
    select
      case 
        when IsDatabaseMatch = true and IsProductMatch = true then "1 - In Both - Products Align"
        when IsDatabaseMatch = true and IsProductMatch = False then "2 - In Both - Products Differ"
        when CexProductTagCode is null then "3 - Not in CEX"
        when EdithProductTagCode is null then "4 - Not in EDITH"
        else "5 - Unknown"
      end as Status,
      if(IsDatabaseMatch = true and IsProductMatch = true, 'Pass', 'Fail') as ProductPass,
      *
    from JoinProduct

  )
  Select * from ProductComparison
  order by 1
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Products Comparison Results

-- COMMAND ----------

select
  'Product' as Dimension, -- Todo: Add the same to all tests.
  Status,
  PeriodStatus,
  MarketStatus,
  count(distinct CexDatabaseId) as DatabaseCount,
  array_agg(distinct CexDatabaseId) as Databases,
  format_number(count(distinct ProductTagCode), "#,###") as ProductCount
from mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison
group by all
order by 1


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2 - In Both - Products Differ

-- COMMAND ----------

Select
  *
from mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison
where Status = '2 - In Both - Products Differ'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3 - Not in CEX

-- COMMAND ----------

Select
    Status,
    CexDatabaseId,
    EdithDatabaseId,
    count(distinct ProductTagCode) as Count,
    array_agg(distinct ProductTagCode) as ProducttagCodes
from mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison
where Status = '3 - Not in CEX'
Group by all
order by 2

-- COMMAND ----------

Select min(dateid), max(dateid)
from mdl_europe_anz_dev.silver_edith_nielsen.fact
where true
and ProductTagCode ='P000000000000796259000000000000025190386'
and DatabaseId = 'NIELSEN_CH_COOKINGPRODUCTS_F';
-- min(dateid)	max(dateid)
-- 2021-08-22	  2021-09-12

-- Select min(Periods.WeekEnd), max(Periods.WeekEnd)
-- from mdl_europe_anz.silver_bdl_nielsen_cex.fact
-- join mdl_europe_anz.silver_bdl_nielsen_cex.period as Periods using(YYYYWW)
-- where DatabaseId ='CEX_CH_COOKINGPRODUCTS_F'

-- COMMAND ----------

with ProductChecked as (
  select
    Product.CexDatabaseId,
    product.EdithDatabaseId,
    Product.ProductTagCode
  from mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison as Product
  where product.CexDatabaseId ='CEX_GB_FACECARE_W'
  and left(product.status, 1) in ('3', '4')
)
select
    ProductChecked.CexDatabaseId as CexDatabaseId,
    ProductChecked.EdithDatabaseId as EdithDatabaseId,
    ProductChecked.ProductTagCode as ProductTagCode,
    CexProducts.ProductTagCode is not null as IsInCexProducts,
    min(Periods.WeekEnd) as FirstDateCexFact,
    max(Periods.WeekEnd) as LastDateCexFact,
    sum(CexFacts.SalesValue) as SalesValueCexFacts,
    EdithProducts.ProductTagCode is not null as IsInEdithProducts,
    min(EdithFacts.DateId) as FirstDateEdithFact,
    max(EdithFacts.DateId) as LastDateEdithFact,
    sum(EdithFacts.SalesValueInEuros) as SalesValueEdithFacts
from ProductChecked
left join mdl_europe_anz.silver_bdl_nielsen_cex.product as CexProducts
  on ProductChecked.CexDatabaseId = CexProducts.DatabaseId
  and ProductChecked.ProductTagCode = CexProducts.ProductTagCode
left join mdl_europe_anz.silver_bdl_nielsen_cex.fact as CexFacts
  on CexProducts.DatabaseId = CexFacts.DatabaseId
  and CexProducts.ProductTagCode = CexFacts.ProductTagCode
left join mdl_europe_anz_dev.silver_edith_nielsen.product as EdithProducts
  on ProductChecked.EdithDatabaseId = EdithProducts.DatabaseId
  and ProductChecked.ProductTagCode = EdithProducts.ProductTagCode
left join mdl_europe_anz_dev.silver_edith_nielsen.fact as EdithFacts
  on EdithProducts.DatabaseId = EdithFacts.DatabaseId
  and EdithProducts.ProductTagCode = EdithFacts.ProductTagCode
left join mdl_europe_anz.silver_bdl_nielsen_cex.period as Periods
  on CexFacts.YYYYWW = Periods.YYYYWW
group by all

-- COMMAND ----------

select
  CexDatabaseId,
  EdithDatabaseId,
  case left(Status, 1)
    when "3" then "Products not in Cex Product Table"
    when "4" then "Products not in Edith Product Table"
    else "Unexpected Error"
  end as Status,
  count(distinct ProductTagCode) as ProductCount,
  array_agg(ProductTagCode) as ProducttagCodes
from
  mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison
  where true
    -- and CexDatabaseId = 'CEX_BE_COOKINGPRODUCTS_W'
    and left(Status, 1) <> '1'
group by 1, 2, 3
order by 1, 2

-- COMMAND ----------

Select
    product.Status,
    product.CexDatabaseId,
    product.EdithDatabaseId,
    product.PeriodStatus,
    count(distinct ProductTagCode) as Count,
    array_agg(distinct ProductTagCode) as ProducttagCodes,
    struct(
        RefreshStatus,
        RefreshDelay,
        FirstDate,
        FirstDateDatabase,
        OverlapFirstDate,
        OverlapLastDate,
        LastDate,
        LastDateDatabase,
        OverlapPeriodDiff
    ) as PeriodStatusReason
from mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison as product
left join mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison as period
    on period.CexDatabaseId = product.CexDatabaseId
where Product.Status = '3 - Not in CEX'
Group by all
order by 2

-- COMMAND ----------

Select * from mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison
where CexDatabaseId = ""

-- COMMAND ----------

with cte_product as (
  select 
    product.CexDatabaseId,
    product.EdithDatabaseId,
    product.ProductTagCode,
    period.EdithFirstDate,
    period.EdithLastDate,
    period.CexFirstDate,
    period.CexLastDate
  from
    mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison as product
    left join mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison as period using(EdithDatabaseId)
  where product.CexDatabaseId in ('CEX_ES_DEODORANT_W', 'NIELSEN_ES_DEODORANT_W') 
)
, fact as (
  select 
    EdithDatabaseId,
    product.ProductTagCode,
    product.EdithFirstDate,
    product.EdithLastDate,
    product.CexFirstDate,
    product.CexLastDate,
    concat(Min(f.DateId), " - ", max(f.DateId)) as ProductDateRange
  from
    cte_product as product
    left join  mdl_europe_anz_dev.silver_edith_nielsen.fact as f
      on f.DatabaseId = product.EdithDatabaseId
      and f.ProductTagCode = product.ProductTagCode
  group by 1, 2, 3, 4, 5, 6
)
select * from fact

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4 - Not in EDITH

-- COMMAND ----------

Select
    Status,
    CexDatabaseId,
    EdithDatabaseId,
    count(distinct ProductTagCode) as Count,
    array_agg(distinct ProductTagCode) as ProducttagCodes
from mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison
where Status = '4 - Not in EDITH'
Group by all
order by 2

-- COMMAND ----------

with cte_product as (
  select 
    CexDatabaseId,
    product.ProductTagCode,
    period.OverlapFirstDate,
    period.OverlapLastDate
  from
    mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison as product
    join mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison as period using(CexDatabaseId)
  where true
    -- and CexDatabaseId in ('CEX_ES_DEODORANT_W', 'NIELSEN_ES_DEODORANT_W') 
    and product.status = '4 - Not in EDITH'
)
select 
  CexDatabaseId,
  product.ProductTagCode,
  product.OverlapLastDate,
  min(p.WeekEnd) as ProductFirstDate,
  max(p.WeekEnd) as ProductLastDate
from
  cte_product as product
  left join  mdl_europe_anz.silver_bdl_nielsen_cex.fact as f
    on f.DatabaseId = product.CexDatabaseId
    and f.ProductTagCode = product.ProductTagCode
  left join mdl_europe_anz.silver_bdl_nielsen_cex.period as p using (YYYYWW)
group by 1, 2, 3;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5 - Unknown

-- COMMAND ----------

Select
  *
from mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison
where Status = '5 - Unknown'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Product Comparison Test Queries

-- COMMAND ----------

with PassingProducts as (
  Select distinct
    CexDatabaseID,
    EdithDatabaseId,
    productTagCode
  from mdl_europe_anz_dev.silver_edith_nielsen.productcomparison
  where true
    and ProductPass = 'Pass'
    and CexDatabaseId = 'CEX_HU_ICECREAM_W'
)
select 
*
from mdl_europe_anz_dev.silver_edith_nielsen.productcomparison
where CexDatabaseId = 'CEX_HU_ICECREAM_W';


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View - EDITH ProductMappingCheck
-- MAGIC * Check column mapping as follows:
-- MAGIC   * If all value are null -> _null_ (Column not mapped)
-- MAGIC   * PercentageMapped = ValueMapped / Total<br>
-- MAGIC With
-- MAGIC     * ValueMapped = count of Columns not _null_ and not '**UAOL**'
-- MAGIC <br>
-- MAGIC * Columns checked:
-- MAGIC   * HierarchyExternalGroup['Company']
-- MAGIC   * HierarchyExternalGroup['Brand']
-- MAGIC   * HierarchyExternalGroup['Segment']
-- MAGIC   * HierarchyExternalGroup['Format']
-- MAGIC   * HierarchyExternalGroup['Benefit']
-- MAGIC   * HierarchyExternalGroup['PackSize']
-- MAGIC   * HierarchyExternalGroup['Variant']
-- MAGIC   * HierarchyExternalGroup['SKUName']
-- MAGIC   * HierarchyExternalGroup['EAN']

-- COMMAND ----------

create or replace table mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck as (
  with Step1 as (
    select distinct 
      p.databaseId as EdithDatabaseId,
      -- d.CexDatabaseId as CexDatabaseId,
      Count(ProductTagCode) as ProductCount,
      SUM(if(HierarchyExternalGroup['Company'] is null or HierarchyExternalGroup['Company'] = 'UAOL', 0, 1)) as CompanyMapped,
      SUM(if(HierarchyExternalGroup['Brand'] is null or HierarchyExternalGroup['Brand'] = 'UAOL', 0, 1)) as BrandMapped,
      SUM(if(HierarchyExternalGroup['Segment'] is null or HierarchyExternalGroup['Segment'] = 'UAOL', 0, 1)) as SegmentMapped,
      SUM(if(HierarchyExternalGroup['Format'] is null or HierarchyExternalGroup['Format'] = 'UAOL', 0, 1)) as FormatMapped,
      SUM(if(HierarchyExternalGroup['Benefit'] is null or HierarchyExternalGroup['Benefit'] = 'UAOL', 0, 1)) as BenefitMapped,
      SUM(if(HierarchyExternalGroup['PackSize'] is null or HierarchyExternalGroup['PackSize'] = 'UAOL', 0, 1)) as PackSizeMapped,
      SUM(if(HierarchyExternalGroup['Variant'] is null or HierarchyExternalGroup['Variant'] = 'UAOL', 0, 1)) as VariantMapped,
      SUM(if(HierarchyExternalGroup['SKUName'] is null or HierarchyExternalGroup['SKUName'] = 'UAOL', 0, 1)) as SKUNameMapped,
      SUM(if(HierarchyExternalGroup['EAN'] is null or HierarchyExternalGroup['EAN'] = 'UAOL', 0, 1)) as EANMapped,
      count(HierarchyExternalGroup['Company']) as CompanyCnt,
      count(HierarchyExternalGroup['Brand']) as BrandCnt,
      count(HierarchyExternalGroup['Segment']) as SegmentCnt,
      count(HierarchyExternalGroup['Format']) as FormatCnt,
      count(HierarchyExternalGroup['Benefit']) as BenefitCnt,
      count(HierarchyExternalGroup['PackSize']) as PackSizeCnt,
      count(HierarchyExternalGroup['Variant']) as VariantCnt,
      count(HierarchyExternalGroup['SKUName']) as SKUNameCnt,
      count(HierarchyExternalGroup['EAN']) as EANCnt
    from
      mdl_europe_anz_dev.silver_edith_nielsen.product as p
      -- left join silver_mdl_dataverse.edf_edith_nielsen_database as d on p.DatabaseId=d.Databaseid
    where p.DatabaseId in (Select distinct databaseId from mdl_europe_anz_dev.gold_edith_nielsen.product)
    group by all
  )
  , final as (
    select 
      EdithDatabaseId,
      -- CexDatabaseId,
      Cast(IF(CompanyCnt = 0, null, CompanyMapped / ProductCount * 100) as Decimal(5,2)) as CompanyMappedPercent,
      Cast(IF(BrandCnt = 0, null, BrandMapped / ProductCount * 100) as Decimal(4, 1)) as BrandMappedPercent,
      Cast(IF(SegmentCnt = 0, null, SegmentMapped / ProductCount * 100) as Decimal(4, 1)) as SegmentMappedPercent,
      Cast(IF(FormatCnt = 0, null, FormatMapped / ProductCount * 100) as Decimal(4, 1)) as FormatMappedPercent,
      Cast(IF(BenefitCnt = 0, null, BenefitMapped / ProductCount * 100) as Decimal(5,2)) as BenefitMappedPercent,
      Cast(IF(PackSizeCnt = 0, null, PackSizeMapped / ProductCount * 100) as Decimal(4, 1)) as PackSizeMappedPercent,
      Cast(IF(VariantCnt = 0, null, VariantMapped / ProductCount * 100) as Decimal(4, 1)) as VariantMappedPercent,
      Cast(IF(SKUNameCnt = 0, null, SKUNameMapped / ProductCount * 100) as Decimal(4, 1)) as SKUNameMappedPercent,
      Cast(IF(EANCnt = 0, null, EANMapped / ProductCount * 100) as Decimal(4, 1)) as EANMappedPercent
    from step1
  )
  select * from final
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Product Mapping Check Results

-- COMMAND ----------

select 
  *
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CompanyMappedPercent

-- COMMAND ----------

select 
  EdithDatabaseId,
  CompanyMappedPercent
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### BrandMappedPercent

-- COMMAND ----------

select 
  EdithDatabaseId,
  BrandMappedPercent
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SegmentMappedPercent

-- COMMAND ----------

select 
  EdithDatabaseId,
  SegmentMappedPercent
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### FormatMappedPercent

-- COMMAND ----------

select 
  EdithDatabaseId,
  FormatMappedPercent
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### BenefitMappedPercent

-- COMMAND ----------

select 
  EdithDatabaseId,
  BenefitMappedPercent
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PackSizeMappedPercent

-- COMMAND ----------

select 
  EdithDatabaseId,
  PackSizeMappedPercent
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### VariantMappedPercent

-- COMMAND ----------

select 
  EdithDatabaseId,
  VariantMappedPercent
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SKUNameMappedPercent

-- COMMAND ----------

select 
  EdithDatabaseId,
  SKUNameMappedPercent
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EANMappedPercent

-- COMMAND ----------

select 
  EdithDatabaseId,
  EANMappedPercent
from mdl_europe_anz_dev.silver_edith_nielsen.ProductMappingCheck
order by 2 desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Facts
-- MAGIC Are Databases having the same Fact Sales Value in CEX and EDITH?<br>
-- MAGIC Checks, for databases that passed the mapping test, if their Sales Value match between EDITH and CEX.<br>
-- MAGIC Partition:
-- MAGIC * Database
-- MAGIC * Market
-- MAGIC <br>
-- MAGIC <br>
-- MAGIC Comparison:
-- MAGIC * Sum Sales Value 
-- MAGIC * Min Sales Value
-- MAGIC * Max Sales Value
-- MAGIC * Avg Sales Value
-- MAGIC * count(*) by partition,
-- MAGIC * count(distinct period)
-- MAGIC * count(distinct product)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## View - FactComparison

-- COMMAND ----------

Create or replace table mdl_europe_anz_dev.silver_edith_nielsen.FactComparison as (
  with PassedDatabases as (
    Select distinct
      product.CexDatabaseID,
      product.EdithDatabaseId,
      OverlapFirstDate,
      OverlapLastDate
    from mdl_europe_anz_dev.silver_edith_nielsen.ProductComparison as product
    join mdl_europe_anz_dev.silver_edith_nielsen.PeriodComparison as period on
      product.CexDatabaseID = period.CexDatabaseID
    where 
      ProductPass = 'Pass'
      and left(period.status, 1) in ('1', '2')
  )
  -- Consider only overlapping periods for each database (Recursive cte)
  -- Overlaping periods between CEX and EDITH
  -- Check if it is group by Period. 
  , CexFact as (
    Select
      f.databaseid as CexDatabaseId,
      db.EdithDatabaseId as EdithDatabaseId,
      f.MarketTagCode as MarketTagCode,
      m.MarketLong as MarketLong,
      f.ProducttagCode as ProducttagCode,
      p.ProductLong as ProductLong,
      count(*) as CexCount,
      sum(f.SalesValue) as CexSalesValue,
      min(f.SalesValue) as CexMinValue,
      max(f.SalesValue) as CexMaxValue,
      avg(f.SalesValue) as CexAvgValue
      -- count(distinct f.YYYYWW) as CexPeriodCount,
      -- count(distinct f.ProductTagCode) as CexProductCount
    from 
      mdl_europe_anz.silver_bdl_nielsen_cex.fact as f
      inner join PassedDatabases as db on 
        f.DatabaseId = db.CexDatabaseId
      inner join mdl_europe_anz.silver_bdl_nielsen_cex.period as d
        on f.YYYYWW = d.YYYYWW 
      inner join mdl_europe_anz.silver_bdl_nielsen_cex.product as p on 
        p.DatabaseId = f.DatabaseId
        and p.ProductTagCode=f.ProductTagCode
        and p.HierarchyNumber = 1
        and p.IsMaxLevel = true
      inner join mdl_europe_anz.silver_bdl_nielsen_cex.market as m on 
        m.DatabaseId = f.DatabaseId
        and m.MarketTagCode = f.MarketTagCode
    where  d.WeekEnd between db.OverlapFirstDate and db.OverlapLastDate 
    group by 1, 2, 3, 4, 5, 6
  )
  , EdithFact as (
    Select
      db.CexDatabaseID as CexDatabaseId,
      f.Databaseid as EdithDatabaseId,
      f.MarketTagCode as MarketTagCode,
      m.MarketLong as MarketLong,
      f.ProducttagCode as ProducttagCode,
      p.ProductLong as ProductLong,
      count(*) as EdithCount,
      sum(f.SalesValueInEuros) as EdithSalesValue,
      min(f.SalesValueInEuros) as EdithMinValue,
      max(f.SalesValueInEuros) as EdithMaxValue,
      avg(f.SalesValueInEuros) as EdithAvgValue
      -- count(distinct f.DateId) as EdithPeriodCount,
      -- count(distinct f.ProductTagCode) as EdithProductCount
    from 
      mdl_europe_anz_dev.silver_edith_nielsen.fact as f
      inner join PassedDatabases as db on 
        f.DatabaseId = db.EdithDatabaseId
      inner join mdl_europe_anz_dev.silver_edith_nielsen.product as p on
        p.DatabaseId = f.DatabaseId
        and p.ProductTagCode=f.ProductTagCode
        and p.IsUnmaskingRow = false
      inner join mdl_europe_anz_dev.silver_edith_nielsen.market as m on
        m.DatabaseId = f.DatabaseId
        and m.MarketTagCode = f.MarketTagCode
    where f.DateId between db.OverlapFirstDate and db.OverlapLastDate 
    group by 1, 2, 3, 4, 5, 6
  )
  , JoinFact as (
    select
      Coalesce(cex.CexDatabaseId, edith.CexDatabaseId) as CexDatabaseId,
      coalesce(cex.EdithDatabaseId, edith.EdithDatabaseId) as EdithDatabaseId,
      coalesce(cex.MarketTagCode, edith.MarketTagCode) as MarketTagCode,
      coalesce(cex.ProductTagCode, edith.ProducttagCode) as ProductTagCode,
      cex.MarketLong as CexMarketLong,
      edith.MarketLong as EdithMarketLong, 
      cex.ProductLong as CexProductLong,
      edith.ProductLong as EdithProductLong,
      (cex.CexDatabaseId is not null) as InCex,
      (edith.CexDatabaseId is not null) as InEdith,
      (InCex = true and InEdith = true) as IsDatabaseMatch,
      (CexCount = EdithCount) as IsCountMatch,
      (abs(CexSalesValue - EdithSalesValue) < 0.01) as IsTotalMatch,
      (abs(CexMinValue - EdithMinValue) < 0.01) as IsMinMatch,
      (abs(CexMaxValue - EdithMaxValue) < 0.01) as IsMaxMatch,
      (abs(CexAvgValue - EdithAvgValue) < 0.01) as IsAvgMatch,
      -- (CexPeriodCount = EdithPeriodCount) as IsPeriodCountMatch,
      -- (CexProductCount = EdithProductCount) as IsProductCountMatch,
      CexCount,
      CexSalesValue,
      CexMinValue,
      CexMaxValue,
      CexAvgValue,
      -- CexPeriodCount,
      -- CexProductCount,
      EdithCount,
      EdithSalesValue,
      EdithMinValue,
      EdithMAxValue,
      EdithAvgValue
      -- EdithPeriodCount,
      -- EdithProductCount
    from
      CexFact as cex
      full outer join EdithFact as edith on 
        cex.CexDatabaseId = edith.CexDatabaseId
        and cex.MarkettagCode = edith.MarkettagCode
        and cex.ProductTagCode = edith.ProductTagCode
  )
  , FactComparison as (
    select
      case
        when IsDatabaseMatch = true and IsTotalMatch = true then '1 - In Both - Facts Align'
        when IsDatabaseMatch = true and IsTotalMatch = false then '2 - In Both - Values Differ'
        when InCex = false and InEdith = true then '3 - Not in CEX'
        when InCex = true and InEdith = false then '4 - Not in EDITH'
        else '5 - Unknown'
      end as Status,
      if(IsDatabaseMatch = true and IsTotalMatch = true, 'Pass', 'Fail') as FactPass,
      *
    from JoinFact
  )
  select * from FactComparison
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Fact Comparison test Queries

-- COMMAND ----------

with Comparison as (
  select 
    f.CexDatabaseId,
    f.EdithDatabaseId,
    pc.Status as ProductStatus,
    pc.PeriodStatus,
    f.MarketTagCode,
    f.ProductTagCode,
    f.CexMarketLong,
    f.EdithMarketLong,
    f.CexProductLong,
    f.EdithProductLong,
    f.CexCount,
    f.EdithCount,
    try_divide(f.CexCount, f.EdithCount) as CountRatio,
    abs(f.CexSalesValue - f.EdithSalesValue) as SalesDifference,
    struct(CexSalesValue, CexMinValue, CexMaxValue, CexAvgValue) as CexSales,
    struct(EdithSalesValue, EdithMinValue, EdithMAxValue, EdithAvgValue) as EdithSales
  from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison as f
  left join mdl_europe_anz_dev.silver_edith_nielsen.productcomparison as pc on
    pc.CexDatabaseId = f.CexDatabaseId
    and pc.ProductTagCode = f.ProductTagCode
  where true
    and abs(CexSalesValue - EdithSalesValue) >= 0.01

)
select 
  CexDatabaseId,
  EdithDatabaseId,
  ProductStatus,
  PeriodStatus,
  count(*) as DiscrepancyCount,
  CountRatio,
  Count(distinct MarketTagCode) as MarketCount,
  array_agg(distinct struct(MarketTagCode, CexMarketLong, EdithMarketLong)) as MarketList,
  count(distinct ProductTagCode) as ProductCount,
  array_agg(distinct struct(ProductTagCode, CexProductLong, EdithProductLong, CexCount, EdithCount, CountRatio)) as ProductList
from Comparison
where CexCount <> EdithCount
group by all

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Details for Specific Database

-- COMMAND ----------

select 
  CexDatabaseId,
  EdithDatabaseId,
  MarketTagCode,
  ProductTagCode,
  CexMarketLong,
  EdithMarketLong,
  CexProductLong,
  EdithProductLong,
  CexCount,
  EdithCount,
  abs(CexSalesValue - EdithSalesValue) as SalesDifference,
  struct(CexSalesValue, CexMinValue, CexMaxValue, CexAvgValue) as CexSales,
  struct(EdithSalesValue, EdithMinValue, EdithMAxValue, EdithAvgValue) as EdithSales
from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison
where true
and CexDatabaseId = 'CEX_GR_TOOTHBRUSH_W'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- * Add Currency
--   * edith
-- CurrencyActual
-- CurrencyLocal
-- ExchangeRate
-- InverseExchangeRate

--   * Cex
--   CurrencyActual
-- CurrencyLocal
-- ExchangeRate
-- InverseExchangeRate

with DbSalesValue as (
  select 
    substr(CexDatabaseId, 5, 2) as CountryCode,
    CexDatabaseId,
    EdithDatabaseId,
    MarketTagCode,
    -- ProductTagCode,
    sum(CexCount) as CexCount,
    sum(EdithCount) as EdithCount,
    sum(CexSalesValue) as CexSalesValue, 
    sum(EdithSalesValue) as EdithSalesValue,
    try_divide(sum(CexSalesValue), sum(EdithSalesValue)) as SalesValueCoeff
  from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison
  where abs(CexSalesValue - EdithSalesValue) > 1
  group by all
)
, DbGranularity as ( 
  Select 
      CountryCode,
      CexDatabaseId,
      EdithDatabaseId,
      sum(CexCount) as CexCount,
      sum(EdithCount) as EdithCount,
      count(MarketTagCode) as Markets,
      abs(sum(CexSalesValue) - sum(EdithSalesValue)) as SalesDifference,
      min(SalesValueCoeff) as MinCoeff,
      max(SalesValueCoeff) as MaxCoeff,
      avg(SalesValueCoeff) as AvgCoeff
  from
    DbSalesValue
  group by all
)
, MarketGranularity as ( 
  Select 
      CountryCode,
      CexDatabaseId,
      EdithDatabaseId,
      MarketTagCode,
      count(MarketTagCode) as MarketsCount,
      sum(CexCount) as CexCount,
      sum(EdithCount) as EdithCount,
      abs(sum(CexSalesValue) - sum(EdithSalesValue)) as SalesDifference,
      min(SalesValueCoeff) as MinCoeff,
      max(SalesValueCoeff) as MaxCoeff,
      avg(SalesValueCoeff) as AvgCoeff
  from
    DbSalesValue
  group by all
)
, test1 as (
  select * from DbGranularity
  where true
    -- and CountryCode = 'GB'
    -- and CexDatabaseId = 'CEX_HU_ICECREAM_W'
)
select * from DbSalesValue
-- order by CexDatabaseId, MarketTagCode

-- COMMAND ----------


with ParametersCheck as (
  select Distinct
    CexDatabaseId,
    EdithDatabaseId,
    MarketTagCode,
    ProductTagCode
  from
    mdl_europe_anz_dev.silver_edith_nielsen.factcomparison
  where 
    CexCount <> EdithCount
)
, cex as (
  select distinct
    f.databaseid as CexDatabaseId,
    pc.EdithDatabaseId as EdithDatabaseId,
    f.MarketTagCode as MarketTagCode,
    f.ProductTagCode as ProductTagCode,
    pr.WeekEnd as Period,
    f.SalesValue as CexSalesValue,
    0 as EdithSalesValue
    -- , CurrencyActual as CexCurrencyActual,
    -- CurrencyLocal as CexCurrencyLocal,
    -- ExchangeRate as CexExchangeRate,
    -- InverseExchangeRate as CexInverseExchangeRate
  from ParametersCheck as pc
  join  mdl_europe_anz.silver_bdl_nielsen_cex.fact as f on
    pc.CexDatabaseId = f.DatabaseId
    and pc.MarketTagCode = f.MarketTagCode
    and pc.ProductTagCode = f.ProductTagCode
  join mdl_europe_anz.silver_bdl_nielsen_cex.product as p on
    p.DatabaseId = f.DatabaseId
    and p.ProductTagCode = f.ProductTagCode
  join mdl_europe_anz.silver_bdl_nielsen_cex.market as m on
    m.DatabaseId = f.DatabaseId
    and m.MarketTagCode = f.MarketTagCode
  join mdl_europe_anz.silver_bdl_nielsen_cex.period as pr on
    pr.YYYYWW = f.YYYYWW
)
, edith as (
  select distinct
    pc.CexDatabaseId as CexDatabaseId,
    f.databaseid as EdithDatabaseId,
    f.MarketTagCode as MarketTagCode,
    f.ProductTagCode as ProductTagCode,
    f.dateid as Period,
    0 as CexSalesValue,
    f.SalesValueInEuros as EdithSalesValue
    -- , CurrencyActual as EdithCurrencyActual,
    -- CurrencyLocal as EdithCurrencyLocal,
    -- ExchangeRate as EdithExchangeRate,
    -- InverseExchangeRate as EdithInverseExchangeRate
  from ParametersCheck as pc
  join mdl_europe_anz_dev.silver_edith_nielsen.fact as f on 
    pc.EdithDatabaseId = f.DatabaseId
    and pc.MarketTagCode = f.MarketTagCode
    and pc.ProductTagCode = f.ProductTagCode
  join mdl_europe_anz_dev.silver_edith_nielsen.product as p on
    p.DatabaseId = f.DatabaseId
    and p.ProductTagCode = f.ProductTagCode
  join mdl_europe_anz_dev.silver_edith_nielsen.market as m on
    m.DatabaseId = f.DatabaseId
    and m.MarketTagCode = f.MarketTagCode
)
, FactUnion as (
  select * from cex
  union
  SELECT * FROM edith
)
select
  CexDatabaseId,
  EdithDatabaseId,
  MarketTagCode,
  ProductTagCode,
  Period,
  sum(CexSalesValue) as CexSalesValue,
  sum(EdithSalesValue) as EdithSalesValue
from FactUnion
group by 1, 2, 3, 4, 5
having abs(sum(CexSalesValue) - sum(EdithSalesValue)) >= 0.01
order by 1, 3, 4, 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Non Matching facts

-- COMMAND ----------

select 
  CexDatabaseId,
  EdithDatabaseId,
  abs(sum(CexSalesValue) - sum(EdithSalesValue)) as SalesDifference,
  count(distinct MarketTagCode) as MarketCount,
  array_agg(distinct struct(MarketTagCode, CexMarketLong, EdithMarketLong)) as Markets,
  count(distinct ProductTagCode) as ProductCount,
  array_agg(distinct struct(ProductTagCode, CexProductLong, EdithProductLong))  as Product

from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison
where true
  -- and CexDatabaseId = 'CEX_GB_COOKINGPRODUCTS_W'
  and IsTotalMatch = false
  -- and (CexMarketLong <> EdithMarketLong or CexProductLong <> EdithProductLong)
group by 1, 2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Fact Details for specific database

-- COMMAND ----------

with edith as (
  Select 
    'CEX_GB_COOKINGPRODUCTS_W' as CexDatabaseId,
    f.DatabaseId as EdithDatabaseId,
    f.MarketTagCode,
    m.MarketLong,
    f.ProductTagCode,
    p.ProductLong,
    f.PeriodTagCode,
    f.ExchangeRate,
    f.YYYYWW,
    0 as CexRecord,
    1 as EdithRecord,
    SalesValueInEuros as EdithSalesValue,
    0 as CEXSalesValue
  from 
    mdl_europe_anz_dev.silver_edith_nielsen.fact as f
    join mdl_europe_anz_dev.silver_edith_nielsen.market as m on 
      f.DatabaseId = m.DatabaseId
      and m.MarketTagCode = f.MarketTagCode
    join mdl_europe_anz_dev.silver_edith_nielsen.product as p on
      p.DatabaseId = f.DatabaseId
      and p.ProductTagCode = f.ProductTagCode
  where 
    f.DatabaseId in ('CEX_GB_COOKINGPRODUCTS_W', 'NIELSEN_GB_COOKINGPRODUCTS_W')
    and f.IsUnmaskingRow = false
    -- and f.MarketTagCode = 'M000000000000108509300000000000001342468'
    -- and f.productTagCode = 'P000000000000658240000000000000172955314'
    -- and f.YYYYWW = '202420'
)
, cex as (
  Select 
    f.DatabaseId as CexDatabaseId,
    'NIELSEN_GB_COOKINGPRODUCTS_W' as EdithDatabaseId,
    f.MarketTagCode,
    m.MarketLong,
    f.ProductTagCode,
    p.ProductLong,
    f.PeriodTagCode,
    f.ExchangeRate,
    f.YYYYWW,
    1 as CexRecord,
    0 as EdithRecord,
    0 as EdithSalesValue,
    SalesValue as CEXSalesValue
  from 
    mdl_europe_anz.silver_bdl_nielsen_cex.fact as f
    join mdl_europe_anz.silver_bdl_nielsen_cex.market as m on 
      f.DatabaseId = m.DatabaseId
      and m.MarketTagCode = f.MarketTagCode
    join mdl_europe_anz.silver_bdl_nielsen_cex.product as p on
      p.DatabaseId = f.DatabaseId
      and p.ProductTagCode = f.ProductTagCode
  where 
    f.DatabaseId in ('CEX_GB_COOKINGPRODUCTS_W', 'NIELSEN_GB_COOKINGPRODUCTS_W') 
    and p.hierarchynumber = 1
    and p.IsMaxLevel = true
    -- and f.MarketTagCode = 'M000000000000108509300000000000001342468'
    -- and f.productTagCode = 'P000000000000658240000000000000172955314'
    -- and f.YYYYWW = '202420'
)
, UnionFacts as (
  select * from cex
  union 
  select * from edith
)
, UnionFactAgg as (
select 
    CexDatabaseId,
    EdithDatabaseId,
    MarketTagCode,
    MarketLong,
    ProductTagCode,
    ProductLong,
    PeriodTagCode,
    YYYYWW,
    sum(CexRecord) as CexRecords,
    sum(EdithRecord) as EdithRecord,
    sum(EdithSalesValue) as EdithSalesValue,
    sum(CEXSalesValue) as CEXSalesValue,
    sum(CEXSalesValue) - sum(EdithSalesValue) as SalesValueDiff,
    abs(sum(CEXSalesValue) - sum(EdithSalesValue)) >= 0.01 as IsRoundingDiff
 from UnionFacts
 group by all
)
select * from UnionFactAgg
where IsRoundingDiff = true
-- order by YYYYWW, 1

-- COMMAND ----------

    
  with tmp as (
    Select distinct
      CexDatabaseId,
      EdithDatabaseId,
      MarketTagCode,
      CexSalesValue,
      EdithSalesValue,
      abs(CexSalesValue - EdithSalesValue) as Difference,
      Difference / CexSalesValue as PercentageDiff
    from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison
    where true
    -- and CexDatabaseId = 'CEX_GR_MUSTARD_W'
    and isTotalMatch = false
    -- 7595050 in CEX
  )
  select
    CexDatabaseId,
    substr(CexDatabaseId, 5, 2) in ('GB', 'IE', 'NL', 'BE', 'IT', 'FR', 'DE', 'PL') as IsPriorityMarket,
    array_agg(MarketTagCode) as MarketTagCodes,
    count(MarketTagCode) as Tags,
    sum(Difference) as Difference,
    sum(Difference) / count(MarketTagCode) as Coeff
  from tmp
  where
     CexDatabaseId = 'CEX_HU_DEODORANT_W'
     and MarketTagCode = 'M000000000000112049100000000000001704217'

  group by 1
  having sum(Difference) / count(MarketTagCode) > 1
  order by 3 DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Facts Comparison Results

-- COMMAND ----------

select
  Status,
  count(distinct CexDatabaseId) as DatabaseCount,
  Count(distinct MarketTagCode) as MarketCount,
  count(distinct ProductTagCode) as ProductCount,
  array_agg(distinct CexDatabaseId) as DatabaseList
from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison
group by 1
order by 1


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2 - In Both - Values Differ

-- COMMAND ----------

Select
    Status,
    FactPass,
    CEXDatabaseID,
    EdithDatabaseId,
    CEXSalesValue,
    EdithSalesValue,
    abs(CEXSalesValue - EdithSalesValue) as ValueDifference,
    try_divide(CEXSalesValue, EdithSalesValue) as SalesRatio
from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison
where Status = '2 - In Both - Values Differ'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3 - Not in CEX

-- COMMAND ----------

Select
    Status,
    FactPass,
    CEXDatabaseID,
    EdithDatabaseId,
    CEXSalesValue,
    EdithSalesValue
from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison
where Status = '3 - Not in CEX'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4 - Not in EDITH

-- COMMAND ----------

Select
    Status,
    FactPass,
    CEXDatabaseID,
    EdithDatabaseId,
    CEXSalesValue,
    EdithSalesValue
from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison
where Status = '4 - Not in EDITH'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5 - Unknown

-- COMMAND ----------

Select
    Status,
    FactPass,
    CEXDatabaseID,
    EdithDatabaseId,
    CEXSalesValue,
    EdithSalesValue
from mdl_europe_anz_dev.silver_edith_nielsen.FactComparison
where Status = '5 - Unknown'

-- COMMAND ----------


