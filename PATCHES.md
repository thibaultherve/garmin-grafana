# Patches on garmin-grafana

This fork adds the following data extraction patches to `garmin_fetch.py`:

## 1. Training Effect Label
Exposes `trainingEffectLabel` in `ActivitySummary` — classifies each activity as
AEROBIC_BASE, TEMPO, LACTATE_THRESHOLD, VO2MAX, ANAEROBIC_CAPACITY, or SPRINT.

## 2. Monthly Load Distribution
Adds `monthlyLoadLow`, `monthlyLoadMedium`, `monthlyLoadHigh` (aerobic) and
`monthlyLoadAnaerobic` to `TrainingStatus`.

## 3. Training Balance Feedback
Exposes `trainingBalanceFeedbackPhrase` in `TrainingStatus` — Garmin official
training balance assessment string.

## 4. HR & Power Zones Snapshots
Daily snapshots of `HRZones` and `PowerZones` measurements with zone floors,
max HR, resting HR, LTHR, and FTP.

## 5. Heat & Altitude Acclimation
New `HeatAltitudeAcclimation` measurement with heat/altitude acclimation
percentage and trend.

## 6. HRV Status
New `HRVStatus` measurement with weekly average, last night average,
baseline band (low/balanced upper), and status (BALANCED/UNBALANCED/LOW).

## 7. Workout Steps & Targets
Extracts prescribed workout structure from .fit files into `WorkoutStep` and
writes per-second `WorkoutTarget` fields (TargetLow/High for HR, Power,
Cadence) to `ActivityGPS` for prescribed-vs-actual overlay in dashboards.
