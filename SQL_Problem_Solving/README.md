# Machine Event Processing – SQL Solution

This project processes machine event logs to correctly handle:

1. Shift changes
2. Break periods
3. Events fully inside breaks

The goal is to generate a clean machine activity timeline after applying these rules.

---

# Tools Used

- Database: MySQL 8.0 Community Edition
- SQL Client: MySQL Workbench
- Data Import Method: Table Data Import Wizard (CSV import)

The input datasets were imported into MySQL tables using the **Table Data Import Wizard available in MySQL Workbench**.

---

# Input Tables

The solution uses three input tables:

### 1️⃣ event
Represents machine events recorded with start and end timestamps.

| Column | Description |
|------|-------------|
| device_id | Machine identifier |
| event_id | Unique event id |
| event_start_time | Event start time |
| event_end_time | Event end time |

---

### 2️⃣ shift
Represents machine operating shifts.

| Column | Description |
|------|-------------|
| device_id | Machine identifier |
| shift_code | Shift label |
| shift_start_time | Shift start time |
| shift_end_time | Shift end time |

---

### 3️⃣ break_table
Represents break periods within a shift.

| Column | Description |
|------|-------------|
| device_id | Machine identifier |
| shift_code | Shift identifier |
| break_start_time | Break start |
| break_end_time | Break end |
| break_type | Type of break |

---

# Problem Statement

Machine events may overlap with:

- shift boundaries
- break periods

These situations must be handled correctly so that the final event timeline represents **actual machine operating time**.

---

# Solution Approach

The solution was implemented in three logical steps.

---

# Question 1  
## Split Events When Shift Changes

If an event overlaps with multiple shifts, the event must be clipped to the shift boundaries.

Example

Event  
06:50 → 07:10  

Shift  
07:00 → 15:00  

Correct Output  

07:00 → 07:10  

This is implemented using SQL functions:
- `GREATEST()` → ensures event start does not occur before shift start
- `LEAST()` → ensures event end does not exceed shift end

Output Table:
select * from event_modified


---

# Question 2  
## Split Events Around Break Periods

If an event overlaps with a break period, it must be split into two segments.

Example

Event  
09:10 → 11:30  

Break  
09:00 → 09:30  

Result

Event Part 1  
09:10 → 09:30  

Event Part 2  
09:30 → 11:30  

Steps performed:

1. Detect event and break overlap
2. Extract portion before break
3. Extract portion after break
4. Identify events not touching breaks
5. Merge all segments

Output Table:
select * from event_break_split


---

# Question 3  
## Remove Events Fully Inside Break Periods

If an event occurs completely during a break, it should be removed because the machine is not operational during that time.

Example

Break  
10:00 → 10:15  

Event  
10:05 → 10:10  

Result  
Event removed.

Final Output Table:
select * from final_machine_event


---

# Final Pipeline Flow
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
final_machine_event


