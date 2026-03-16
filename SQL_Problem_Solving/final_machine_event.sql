create database iot_records;
use iot_records;
/* =====================================================
Machine Event Processing – SQL Solution
Goal:
Process machine event logs to correctly handle

1. Shift changes
2. Break periods
3. Events fully inside breaks

The final output should represent the correct
machine activity timeline.
===================================================== */



/* =====================================================
STEP 0 – SCHEMA INSPECTION
Before starting transformation we inspect the schema
to understand column types and formatting issues.
===================================================== */

DESCRIBE break_table;
DESCRIBE shift;
DESCRIBE event;



/* =====================================================
STEP 1 – DATA FORMAT CLEANING
Some timestamp fields are stored as text and must
be converted to proper DATETIME format.
===================================================== */


/* ---------- BREAK TABLE CLEANING ---------- */

SELECT break_start_time, break_end_time
FROM break_table
LIMIT 10;

-- Check invalid timestamps
SELECT break_start_time
FROM break_table
WHERE STR_TO_DATE(break_start_time,'%Y-%m-%d %H:%i:%s.%f') IS NULL;

SELECT break_end_time
FROM break_table
WHERE STR_TO_DATE(break_end_time,'%Y-%m-%d %H:%i:%s.%f') IS NULL;

-- Check logical timestamp errors
SELECT *
FROM break_table
WHERE STR_TO_DATE(break_end_time,'%Y-%m-%d %H:%i:%s.%f')
      <
      STR_TO_DATE(break_start_time,'%Y-%m-%d %H:%i:%s.%f');


-- Convert timestamp columns
ALTER TABLE break_table
MODIFY break_start_time DATETIME,
MODIFY break_end_time DATETIME;


-- Convert shift_day

UPDATE break_table
SET shift_day = STR_TO_DATE(shift_day,'%d-%m-%Y %H.%i');

ALTER TABLE break_table
MODIFY shift_day DATETIME,
MODIFY shift_code CHAR(1),
MODIFY break_type VARCHAR(50);



/* ---------- SHIFT TABLE CLEANING ---------- */

SELECT shift_start_time, shift_end_time
FROM shift
LIMIT 10;

UPDATE shift
SET shift_start_time = STR_TO_DATE(shift_start_time,'%d-%m-%Y %H.%i'),
    shift_end_time = STR_TO_DATE(shift_end_time,'%d-%m-%Y %H.%i');

UPDATE shift
SET shift_day = STR_TO_DATE(shift_day,'%d-%m-%Y %H.%i');

ALTER TABLE shift
MODIFY shift_id VARCHAR(50),
MODIFY shift_day DATETIME,
MODIFY shift_code CHAR(1),
MODIFY shift_start_time DATETIME,
MODIFY shift_end_time DATETIME;


-- Validate shift timestamps
SELECT *
FROM shift
WHERE shift_start_time IS NULL
   OR shift_end_time IS NULL;



/* ---------- EVENT TABLE CLEANING ---------- */

SELECT event_start_time, event_end_time
FROM event
LIMIT 10;

-- Remove ISO format characters
UPDATE event
SET event_start_time = REPLACE(event_start_time,'T',' ');

UPDATE event
SET event_start_time = REPLACE(event_start_time,'Z','');

UPDATE event
SET event_end_time = REPLACE(event_end_time,'T',' ');

UPDATE event
SET event_end_time = REPLACE(event_end_time,'Z','');

-- Convert to DATETIME
ALTER TABLE event
MODIFY event_start_time DATETIME,
MODIFY event_end_time DATETIME,
MODIFY shift_code CHAR(1),
MODIFY shift_id VARCHAR(50);

-- Validate event timestamps
SELECT COUNT(*)
FROM event
WHERE event_end_time < event_start_time;



/* =====================================================
STEP 2 – DATA QUALITY CHECKS
Ensure no invalid records exist before transformation
===================================================== */

-- Events with invalid duration 
-- I observed that the timestamps were valid and not negative, and such events are common in machine telemetry where signals occur at a single moment rather than over a duration. Therefore I treated them as valid instantaneous events.

SELECT *
FROM event
WHERE event_start_time >= event_end_time;

-- Breaks with invalid duration
SELECT *
FROM break_table
WHERE break_start_time >= break_end_time;

/* =====================================================
NOTE ON PERFORMANCE
In production systems indexes are added on join columns.
Since this dataset is small (interview assignment),
indexes are intentionally not created.
===================================================== */

/* =====================================================
QUESTION 1
Split events when shift changes
=====================================================

If an event crosses a shift boundary,
the event is clipped to the shift time.

Example

Event
06:50 → 07:10

Shift
07:00 → 15:00

Result
07:00 → 07:10
===================================================== */

CREATE TABLE event_modified AS
SELECT
    e.device_id,
    s.shift_code,
    e.event_id,

    GREATEST(e.event_start_time, s.shift_start_time) AS event_start_time,
    LEAST(e.event_end_time, s.shift_end_time) AS event_end_time

FROM event e
JOIN shift s
ON e.device_id = s.device_id
AND e.event_start_time < s.shift_end_time
AND e.event_end_time > s.shift_start_time;

/* =====================================================
QUESTION 2
Cut events during break periods
=====================================================

If an event overlaps with a break,
the event must be split into two segments.
===================================================== */



/* Step 1 Detect break overlap */

CREATE TABLE event_break_overlap AS
SELECT
    es.device_id,
    es.shift_code,
    es.event_id,
    es.event_start_time,
    es.event_end_time,
    b.break_start_time,
    b.break_end_time

FROM event_modified es
JOIN break_table b
ON es.device_id = b.device_id
AND es.shift_code = b.shift_code
AND es.event_start_time < b.break_end_time
AND es.event_end_time > b.break_start_time;



/* Step 2 Event part before break */

CREATE TABLE event_before_break AS
SELECT
    device_id,
    shift_code,
    event_id,
    event_start_time,
    break_start_time AS event_end_time
FROM event_break_overlap
WHERE event_start_time < break_start_time;



/* Step 3 Event part after break */

CREATE TABLE event_after_break AS
SELECT
    device_id,
    shift_code,
    event_id,
    break_end_time AS event_start_time,
    event_end_time
FROM event_break_overlap
WHERE event_end_time > break_end_time;



/* Step 4 Events without break */

CREATE TABLE event_no_break AS
SELECT
    device_id,
    shift_code,
    event_id,
    event_start_time,
    event_end_time
FROM event_modified es
WHERE NOT EXISTS (
    SELECT 1
    FROM break_table b
    WHERE es.device_id = b.device_id
    AND es.shift_code = b.shift_code
    AND es.event_start_time < b.break_end_time
    AND es.event_end_time > b.break_start_time
);



/* Step 5 Merge event segments */

CREATE TABLE event_break_split AS
SELECT device_id, shift_code, event_id, event_start_time, event_end_time
FROM event_before_break
UNION ALL
SELECT device_id, shift_code, event_id, event_start_time, event_end_time
FROM event_after_break
UNION ALL
SELECT device_id, shift_code, event_id, event_start_time, event_end_time
FROM event_no_break;


/* =====================================================
QUESTION 3
Remove events fully inside break periods
===================================================== */

CREATE TABLE final_machine_event AS
SELECT *
FROM event_break_split es
WHERE NOT EXISTS (
    SELECT 1
    FROM break_table b
    WHERE es.device_id = b.device_id
    AND es.shift_code = b.shift_code
    AND es.event_start_time >= b.break_start_time
    AND es.event_end_time <= b.break_end_time
);

/* =====================================================
FINAL OUTPUT
===================================================== */

SELECT *
FROM final_machine_event
ORDER BY device_id, event_start_time;

/* =====================================================
Conclusion

In this solution, machine event logs were processed to
correctly handle shift boundaries and break periods.

First, events were aligned with shift timings to ensure
no event extends beyond a shift window.

Next, events overlapping with break periods were split
into separate segments so that machine inactivity during
breaks is properly represented.

Finally, events that occurred completely within break
periods were removed because the machine is not
operational during that time.

The resulting dataset (machine_event_timeline) provides
a clean and accurate timeline of machine activity and
can be used for further analytics such as production
monitoring, downtime analysis, and operational reporting.

event
   ↓
event_modified
   ↓
event_break_overlap
   ↓
event_before_break
event_after_break
event_no_break
   ↓
event_break_split
   ↓
Final_machine_event
===================================================== */

/* =====================================================
3 outputs corresponding to the 3 questions.
===================================================== */

select * from event_modified;
select * from event_break_split;
select * from final_machine_event;
