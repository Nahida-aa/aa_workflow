//! Schedule 物化器：把注册期声明的 schedule 换算成 store 里的记录。
//!
//! 对齐 TanStack `workflow-runtime/src/schedule-materializer.ts`（272 行）。
//!
//! # 为什么需要它
//!
//! [`every::minutes(15)`](crate::every::minutes) / [`cron`](crate::cron) 产出的
//! [`WorkflowScheduleSpec`] 只是**声明**——`sweep` 的
//! `claim_due_schedule_buckets` 要的是 store 里有 `next_fire_at` 的 schedule
//! 记录。物化器负责这步换算，**每次 sweep 前调一次**（上游的 host adapter
//! 入口就是「物化 → sweep」两步）。
//!
//! # 与上游的三点差异
//!
//! - **不经 runtime 对象**：上游 `materializeWorkflowSchedules(runtime, opts)`
//!   从 runtime 取 `workflows` 与 `store`；我们直接收 store 与注册表，
//!   因为 Rust 的 runtime 不把 config 暴露成字段。
//! - **`input` 是纯值**：TS 允许函数（每轮物化求值）；Rust 是
//!   [`serde_json::Value`]，需要动态 input 的宿主自行构造定义。
//! - **自带 UTC 日历换算**：上游用 JS `Date` 的 `getUTC*()`；我们不引日期库，
//!   用 civil-from-days 算法（`utc_parts`）——只处理 UTC，无时区/locale
//!   复杂度，不值得为几个取值器拉一个完整日期库进来。
//!
//! # cron 子集（与上游一致）
//!
//! 五字段（分 时 日 月 周）、支持 `*` / `N` / `N-M` / `N-M/S` / 逗号列表。
//! **不支持月份名/星期名的英文缩写**（上游也不支持——它只 parse 数字）。
//! 时区只接受 UTC（或缺省）；给别的时区会报错（对齐上游的显式拒绝）。

use serde_json::Value;

use crate::types::{
    ScheduleId, UpsertScheduleArgs, WorkflowOverlapPolicy, WorkflowScheduleDefinition,
    WorkflowScheduleSpec,
};
use crate::{WorkflowExecutionStore, WorkflowRegistration};

/// 默认 cron 回看窗口（对齐上游 `DEFAULT_CRON_LOOKBACK_MS`）：32 天。
///
/// 回看的用途：宿主可能停机一段时间，恢复后要补跑停机期间**本该触发**的那一次
/// （而非直接跳到下一次）。窗口限制了这个补跑的追溯深度。
pub const DEFAULT_CRON_LOOKBACK_MS: i64 = 32 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Default)]
pub struct MaterializeWorkflowSchedulesOptions {
    /// 覆盖当前时间（测试用）；缺省取 `now_ms()`。
    pub now: Option<i64>,
    /// 覆盖 cron 回看窗口；缺省 [`DEFAULT_CRON_LOOKBACK_MS`]。
    pub cron_lookback_ms: Option<i64>,
}

/// 一个 schedule 的物化结果（对齐上游 `MaterializedWorkflowSchedule`）。
#[derive(Debug, Clone, PartialEq)]
pub enum MaterializedWorkflowSchedule {
    /// 已算出 `fire_at` 并登记（`next_fire_at = fire_at`）。
    Materialized {
        workflow_id: String,
        schedule_id: ScheduleId,
        fire_at: i64,
        schedule: WorkflowScheduleSpec,
    },
    /// 声明为停用，已登记为 `enabled: false`（无 `next_fire_at`）。
    Disabled {
        workflow_id: String,
        schedule_id: ScheduleId,
        schedule: WorkflowScheduleSpec,
    },
    /// 在回看窗口内没有该触发的时刻（例如 cron 的「下个月 1 号」）。
    NotDue {
        workflow_id: String,
        schedule_id: ScheduleId,
        schedule: WorkflowScheduleSpec,
    },
}

/// 物化注册表里的全部 schedule。
///
/// 每个声明一条结果；已到期/已过期窗口内的会算出 `fire_at` 并入 store。
pub fn materialize_workflow_schedules(
    store: &dyn WorkflowExecutionStore,
    workflows: &std::collections::HashMap<String, WorkflowRegistration>,
    options: &MaterializeWorkflowSchedulesOptions,
) -> anyhow::Result<Vec<MaterializedWorkflowSchedule>> {
    let now = options.now.unwrap_or_else(now_ms);
    let lookback = options.cron_lookback_ms.unwrap_or(DEFAULT_CRON_LOOKBACK_MS);
    if lookback < 0 {
        return Err(anyhow::anyhow!(
            "Workflow cron lookback must be a non-negative number."
        ));
    }

    let mut materialized = Vec::new();
    // 遍历顺序要稳定（HashMap 无序，排序后与上游的 Object.entries 顺序等价地
    // 可复现）；同时每个 workflow 内的 schedule 按声明序处理。
    let mut workflow_ids: Vec<&String> = workflows.keys().collect();
    workflow_ids.sort();

    for workflow_id in workflow_ids {
        let registration = &workflows[workflow_id];
        for (index, definition) in registration.schedules.iter().enumerate() {
            let schedule_id = schedule_id_of(workflow_id, definition, index);

            if definition.enabled == Some(false) {
                store.upsert_schedule(UpsertScheduleArgs {
                    schedule_id: schedule_id.clone(),
                    workflow_id: workflow_id.clone(),
                    workflow_version: registration.version_override.clone(),
                    schedule: definition.schedule.clone(),
                    overlap_policy: definition
                        .overlap_policy
                        .unwrap_or(WorkflowOverlapPolicy::Skip),
                    input: None,
                    next_fire_at: None,
                    enabled: false,
                    now,
                })?;
                materialized.push(MaterializedWorkflowSchedule::Disabled {
                    workflow_id: workflow_id.clone(),
                    schedule_id,
                    schedule: definition.schedule.clone(),
                });
                continue;
            }

            let Some(fire_at) = due_fire_at(&definition.schedule, now, lookback)? else {
                materialized.push(MaterializedWorkflowSchedule::NotDue {
                    workflow_id: workflow_id.clone(),
                    schedule_id,
                    schedule: definition.schedule.clone(),
                });
                continue;
            };

            store.upsert_schedule(UpsertScheduleArgs {
                schedule_id: schedule_id.clone(),
                workflow_id: workflow_id.clone(),
                workflow_version: registration.version_override.clone(),
                schedule: definition.schedule.clone(),
                overlap_policy: definition
                    .overlap_policy
                    .unwrap_or(WorkflowOverlapPolicy::Skip),
                input: definition.input.clone(),
                next_fire_at: Some(fire_at),
                enabled: true,
                now,
            })?;
            materialized.push(MaterializedWorkflowSchedule::Materialized {
                workflow_id: workflow_id.clone(),
                schedule_id,
                fire_at,
                schedule: definition.schedule.clone(),
            });
        }
    }

    Ok(materialized)
}

/// 缺省 id 为 `{workflowId}:{index}`（对齐上游 `getScheduleId`）。
fn schedule_id_of(
    workflow_id: &str,
    definition: &WorkflowScheduleDefinition,
    index: usize,
) -> ScheduleId {
    definition
        .id
        .clone()
        .unwrap_or_else(|| format!("{workflow_id}:{index}"))
}

/// 算出「该触发的时刻」；`None` = 回看窗口内没有（对齐上游 `getDueFireAt`）。
fn due_fire_at(
    schedule: &WorkflowScheduleSpec,
    now: i64,
    lookback_ms: i64,
) -> anyhow::Result<Option<i64>> {
    match schedule {
        WorkflowScheduleSpec::Interval { every_ms, .. } => {
            if *every_ms <= 0 {
                return Err(anyhow::anyhow!(
                    "Interval workflow schedules must use a positive everyMs."
                ));
            }
            // 对齐上游：向下取整到最近一个间隔边界。
            Ok(Some(now - now.rem_euclid(*every_ms)))
        }
        WorkflowScheduleSpec::Cron { .. } => previous_cron_fire_at(schedule, now, lookback_ms),
    }
}

/// 从 `now` 向前逐分钟回看，找最近一个匹配的时刻。
///
/// 逐分钟穷举是**上游的做法**（不是「算下一次」而是「找上一次」）——语义上更
/// 贴合「补跑停机期间本该触发的那次」。
fn previous_cron_fire_at(
    schedule: &WorkflowScheduleSpec,
    now: i64,
    lookback_ms: i64,
) -> anyhow::Result<Option<i64>> {
    let WorkflowScheduleSpec::Cron {
        expression,
        timezone,
    } = schedule
    else {
        unreachable!("只对 Cron 调用");
    };

    if let Some(tz) = timezone
        && tz != "UTC"
    {
        return Err(anyhow::anyhow!(
            "Workflow cron schedules are materialized in UTC. Received timezone \"{tz}\"."
        ));
    }

    let cron = parse_cron_expression(expression)?;
    let start = floor_to_minute(now);
    let end = start - lookback_ms;

    let mut ts = start;
    while ts >= end {
        if cron.matches(ts) {
            return Ok(Some(ts));
        }
        ts -= 60_000;
    }
    Ok(None)
}

/// 向下取整到分钟（对齐上游 `floorToMinute`）。
fn floor_to_minute(timestamp: i64) -> i64 {
    timestamp - timestamp.rem_euclid(60_000)
}

// ============================================================
// cron 解析与匹配
// ============================================================

struct ParsedCronExpression {
    minute: ParsedCronField,
    hour: ParsedCronField,
    day_of_month: ParsedCronField,
    month: ParsedCronField,
    day_of_week: ParsedCronField,
}

struct ParsedCronField {
    /// 原始字段是否为 `*`——**只有整字段是 `*` 才算通配**（对齐上游：
    /// `*/2` 的 `wildcard` 是 false，因为 field 字符串不是 `"*"`）。
    wildcard: bool,
    values: Vec<i64>,
}

/// 五字段：分 时 日 月 周。
fn parse_cron_expression(expression: &str) -> anyhow::Result<ParsedCronExpression> {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    if fields.len() != 5 {
        return Err(anyhow::anyhow!(
            "Workflow cron schedules must use five fields. Received \"{expression}\"."
        ));
    }
    Ok(ParsedCronExpression {
        minute: parse_cron_field(fields[0], 0, 59)?,
        hour: parse_cron_field(fields[1], 0, 23)?,
        day_of_month: parse_cron_field(fields[2], 1, 31)?,
        month: parse_cron_field(fields[3], 1, 12)?,
        // 星期 0..=7，7 归一成 0（对齐上游 normalizeDayOfWeek）。
        day_of_week: parse_cron_field_with(fields[4], 0, 7, Some(normalize_day_of_week))?,
    })
}

fn parse_cron_field(field: &str, min: i64, max: i64) -> anyhow::Result<ParsedCronField> {
    parse_cron_field_with(field, min, max, None)
}

fn parse_cron_field_with(
    field: &str,
    min: i64,
    max: i64,
    normalize: Option<fn(i64) -> i64>,
) -> anyhow::Result<ParsedCronField> {
    let mut values = Vec::new();
    for part in field.split(',') {
        let (range_part, step_part) = match part.split_once('/') {
            Some((r, s)) => (r, Some(s)),
            None => (part, None),
        };
        let step: i64 = match step_part {
            None => 1,
            Some(s) => s
                .parse()
                .map_err(|_| anyhow::anyhow!("Invalid cron step \"{part}\"."))
                .and_then(|v: i64| {
                    if v > 0 {
                        Ok(v)
                    } else {
                        Err(anyhow::anyhow!("Invalid cron step \"{part}\"."))
                    }
                })?,
        };

        let (start, end) = parse_cron_range(range_part, min, max)?;
        let mut value = start;
        while value <= end {
            values.push(match normalize {
                Some(f) => f(value),
                None => value,
            });
            value += step;
        }
    }
    values.sort_unstable();
    values.dedup();

    Ok(ParsedCronField {
        wildcard: field == "*",
        values,
    })
}

/// `*` / `N` / `N-M`（对齐上游 `parseCronRange`）。
fn parse_cron_range(range: &str, min: i64, max: i64) -> anyhow::Result<(i64, i64)> {
    if range == "*" {
        return Ok((min, max));
    }
    let bounds: Vec<&str> = range.split('-').collect();
    match bounds.len() {
        1 => {
            let v = parse_cron_number(bounds[0], min, max)?;
            Ok((v, v))
        }
        2 => {
            let start = parse_cron_number(bounds[0], min, max)?;
            let end = parse_cron_number(bounds[1], min, max)?;
            if end < start {
                return Err(anyhow::anyhow!("Invalid cron range \"{range}\"."));
            }
            Ok((start, end))
        }
        _ => Err(anyhow::anyhow!("Invalid cron range \"{range}\".")),
    }
}

fn parse_cron_number(value: &str, min: i64, max: i64) -> anyhow::Result<i64> {
    let parsed: i64 = value
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid cron value \"{value}\"."))?;
    if parsed < min || parsed > max {
        return Err(anyhow::anyhow!("Invalid cron value \"{value}\"."));
    }
    Ok(parsed)
}

/// 7 → 0（周日两种写法，对齐上游）。
fn normalize_day_of_week(value: i64) -> i64 {
    if value == 7 { 0 } else { value }
}

impl ParsedCronExpression {
    /// 该 UTC 时刻是否匹配（对齐上游 `matchesCron`）。
    ///
    /// 日与星期的组合语义是 cron 的经典坑：**两个都非通配时取「或」**，
    /// 否则取「与」（POSIX 行为，上游照此实现）。
    fn matches(&self, timestamp: i64) -> bool {
        let (year, month, day, hour, minute, weekday) = utc_parts(timestamp);

        let dom_matches = self.day_of_month.values.contains(&day);
        let dow_matches = self.day_of_week.values.contains(&weekday);
        let day_matches = if !self.day_of_month.wildcard && !self.day_of_week.wildcard {
            dom_matches || dow_matches
        } else {
            dom_matches && dow_matches
        };

        let _ = year;
        self.minute.values.contains(&minute)
            && self.hour.values.contains(&hour)
            && day_matches
            && self.month.values.contains(&month)
    }
}

// ============================================================
// UTC 日历换算（不引日期库）
// ============================================================

/// 把 UTC 毫秒时间戳拆成 `(year, month, day, hour, minute, weekday)`。
///
/// - `month` 是 1..=12（对齐 JS `getUTCMonth() + 1`）
/// - `weekday` 是 0..=6（0 = 周日，对齐 JS `getUTCDay()`）
///
/// 用 Howard Hinnant 的 `civil_from_days` 算法（days since 1970-01-01 →
/// 公历年月日），纯整数运算、无分支预测问题、无依赖。
fn utc_parts(timestamp_ms: i64) -> (i64, i64, i64, i64, i64, i64) {
    let seconds = timestamp_ms.div_euclid(1000);
    let days = seconds.div_euclid(86_400);
    let secs_of_day = seconds.rem_euclid(86_400);

    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;

    // 1970-01-01 是周四 → weekday = (days + 4) mod 7，0 = 周日。
    let weekday = (days + 4).rem_euclid(7);

    let (year, month, day) = civil_from_days(days);
    (year, month, day, hour, minute, weekday)
}

/// days since 1970-01-01 → (year, month, day)。
///
/// 算法出处：Howard Hinnant, *chrono-Compatible Low-Level Date Algorithms*。
/// 以 0000-03-01 为纪元原点，把闰年偏移到最后一个月之后，于是「闰日」总落在
/// 年末，可以用一套线性公式处理。
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    // 移位到 0000-03-01 纪元。
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    // 年内第几天，把闰日算在二月末。
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]，3 月 = 0
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 便于宿主把「已物化」的结果与 sweep 串起来。
pub fn materialized_count(results: &[MaterializedWorkflowSchedule]) -> usize {
    results
        .iter()
        .filter(|r| matches!(r, MaterializedWorkflowSchedule::Materialized { .. }))
        .count()
}

/// `input` 为 [`Value`]（非函数）——上游的 `resolveScheduleInput` 在 Rust 里
/// 是恒等映射，保留这个函数名是为了对照上游时能一眼找到位置。
#[allow(dead_code)]
fn resolve_schedule_input(input: &Option<Value>) -> Option<Value> {
    input.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory_store::InMemoryExecutionStore;
    use crate::types::ClaimDueScheduleBucketsArgs;
    use crate::{WorkflowRegistration, WorkflowScheduleDefinition};
    use std::collections::HashMap;
    use aa_workflow_core::Workflow;

    fn spec_interval(ms: i64) -> WorkflowScheduleSpec {
        WorkflowScheduleSpec::Interval {
            every_ms: ms,
            timezone: None,
        }
    }

    fn spec_cron(expr: &str) -> WorkflowScheduleSpec {
        WorkflowScheduleSpec::Cron {
            expression: expr.into(),
            timezone: None,
        }
    }

    /// 2026-09-14T12:34:56Z 的毫秒时间戳（周一）。
    const T: i64 = 1_789_389_296_000;

    #[test]
    fn utc_parts_decomposes_correctly() {
        // 1970-01-01T00:00:00Z → 周四（weekday 4）
        assert_eq!(utc_parts(0), (1970, 1, 1, 0, 0, 4));
        // 2000-03-01T00:00:00Z（闰年边界附近）
        assert_eq!(utc_parts(951_868_800_000), (2000, 3, 1, 0, 0, 3));
        // 2026-09-14T12:34:56Z
        let (y, mo, d, h, mi, wd) = utc_parts(T);
        assert_eq!((y, mo, d, h, mi), (2026, 9, 14, 12, 34), "年月日时分");
        assert_eq!(wd, 1, "2026-09-14 是周一");
    }

    #[test]
    fn floor_to_minute_truncates_seconds() {
        assert_eq!(floor_to_minute(T), T - 56_000, "56 秒被截掉");
        assert_eq!(floor_to_minute(60_000), 60_000);
        assert_eq!(floor_to_minute(59_999), 0);
    }

    /// interval：向下取整到最近边界（对齐上游 Math.floor 语义）。
    #[test]
    fn interval_fires_at_last_boundary() {
        // every 15min = 900_000ms；now = 12:34:56 → 12:30
        let fire = due_fire_at(&spec_interval(900_000), T, DEFAULT_CRON_LOOKBACK_MS)
            .unwrap()
            .unwrap();
        let (_, _, _, h, m, _) = utc_parts(fire);
        assert_eq!((h, m), (12, 30));
    }

    #[test]
    fn interval_rejects_non_positive() {
        assert!(due_fire_at(&spec_interval(0), T, DEFAULT_CRON_LOOKBACK_MS).is_err());
    }

    /// cron：逐分钟回看找最近匹配（上游是「找上一次」而非「算下一次」）。
    #[test]
    fn cron_finds_previous_match() {
        // 每小时的第 0 分钟
        let fire = due_fire_at(&spec_cron("0 * * * *"), T, DEFAULT_CRON_LOOKBACK_MS)
            .unwrap()
            .unwrap();
        let (_, _, _, h, m, _) = utc_parts(fire);
        assert_eq!((h, m), (12, 0), "12:34 的最近一次整点是 12:00");
    }

    /// 回看窗口外 → None（对齐上游返回 undefined）。
    #[test]
    fn cron_outside_lookback_is_not_due() {
        // 每月 1 号 0 点；回看 1 天 → 9/14 回看不到 9/1
        let one_day = 24 * 60 * 60 * 1000;
        assert!(
            due_fire_at(&spec_cron("0 0 1 * *"), T, one_day)
                .unwrap()
                .is_none(),
            "窗口内没有匹配时刻"
        );
        // 回看 32 天（默认）则能看到 9/1
        let fire = due_fire_at(&spec_cron("0 0 1 * *"), T, DEFAULT_CRON_LOOKBACK_MS)
            .unwrap()
            .unwrap();
        let (_, mo, d, h, m, _) = utc_parts(fire);
        assert_eq!((mo, d, h, m), (9, 1, 0, 0));
    }

    /// 日与星期都非通配 → 取「或」（POSIX 语义，上游照此实现）。
    #[test]
    fn cron_day_and_weekday_are_or_when_both_restricted() {
        // 2026-09-14 是周一、但**不是** 1 号。
        let (_, _, day, _, _, weekday) = utc_parts(T);
        assert_eq!(day, 14);
        assert_eq!(weekday, 1, "周一");

        // 「1 号 或 周一」→ 当天 12:34 应匹配（因为周一），尽管号数不是 1。
        let either = parse_cron_expression("34 12 1 * 1").unwrap();
        assert!(either.matches(T), "两者都限制时取「或」→ 周一命中");

        // 「1 号 且 周一」语义（若为与）则不该命中——用一个只有号匹配的对照：
        // 同表达式在「14 号但不是周一」的时刻应不匹配。
        let (_, _, _, _, _, _) = utc_parts(T);
        let and_like = parse_cron_expression("34 12 1 * 1").unwrap();
        // 9/14 是周一 → 命中；把星期改成周二(2) 则只剩「1 号」且不满足，
        // 由于或语义仍要求星期匹配 → 不命中。
        let tue_only = parse_cron_expression("34 12 1 * 2").unwrap();
        assert!(!tue_only.matches(T), "号≠1 且星期≠周二 → 不匹配");
        let _ = and_like;
    }

    /// 步长 `*/N` 展开正确；`*/2` 的 wildcard 为 false（整字段不是 `*`）。
    #[test]
    fn cron_step_expansion_and_wildcard_flag() {
        let cron = parse_cron_expression("*/15 * * * *").unwrap();
        assert_eq!(cron.minute.values, vec![0, 15, 30, 45]);
        assert!(!cron.minute.wildcard, "*/15 不是整字段通配");
        assert!(cron.hour.wildcard);
    }

    #[test]
    fn cron_range_and_list() {
        let cron = parse_cron_expression("0 9-11 * * 1,3").unwrap();
        assert_eq!(cron.hour.values, vec![9, 10, 11]);
        assert_eq!(cron.day_of_week.values, vec![1, 3]);
    }

    /// 星期 7 归一成 0（对齐上游 normalizeDayOfWeek）。
    #[test]
    fn cron_day_of_week_seven_normalizes_to_zero() {
        let cron = parse_cron_expression("0 0 * * 7").unwrap();
        assert_eq!(cron.day_of_week.values, vec![0]);
        let cron = parse_cron_expression("0 0 * * 0-7").unwrap();
        assert_eq!(cron.day_of_week.values, vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn cron_rejects_bad_input() {
        assert!(parse_cron_expression("* * * *").is_err(), "四字段");
        assert!(parse_cron_expression("* * * * * *").is_err(), "六字段");
        assert!(parse_cron_expression("60 * * * *").is_err(), "分越界");
        assert!(parse_cron_expression("* * * * 8").is_err(), "周越界");
    }

    #[test]
    fn cron_rejects_non_utc_timezone() {
        let spec = WorkflowScheduleSpec::Cron {
            expression: "0 * * * *".into(),
            timezone: Some("Asia/Shanghai".into()),
        };
        let err = due_fire_at(&spec, T, DEFAULT_CRON_LOOKBACK_MS).unwrap_err();
        assert!(
            err.to_string().contains("UTC"),
            "非 UTC 时区应显式拒绝，实际 {err}"
        );
    }

    // ── 物化到 store ────────────────────────────────────────

    fn store_and_workflows(
        schedules: Vec<WorkflowScheduleDefinition>,
    ) -> (
        std::sync::Arc<InMemoryExecutionStore>,
        HashMap<String, WorkflowRegistration>,
    ) {
        let mem = std::sync::Arc::new(InMemoryExecutionStore::default());
        let mut workflows = HashMap::new();
        workflows.insert(
            "digest".to_string(),
            WorkflowRegistration {
                load: std::sync::Arc::new(|| Workflow::new("digest")),
                previous_versions: HashMap::new(),
                version_override: Some("v1".into()),
                schedules,
            },
        );
        (mem, workflows)
    }

    /// 到期的 schedule 被登记（id 缺省推导 + enabled + next_fire_at）。
    #[test]
    fn materializes_due_schedule_into_store() {
        let (store, workflows) = store_and_workflows(vec![WorkflowScheduleDefinition {
            id: None,
            schedule: spec_interval(900_000),
            overlap_policy: None,
            input: Some(serde_json::json!({ "batch": 100 })),
            enabled: None,
        }]);
        let results = materialize_workflow_schedules(
            store.as_ref(),
            &workflows,
            &MaterializeWorkflowSchedulesOptions {
                now: Some(T),
                cron_lookback_ms: None,
            },
        )
        .unwrap();

        assert_eq!(results.len(), 1);
        match &results[0] {
            MaterializedWorkflowSchedule::Materialized {
                workflow_id,
                schedule_id,
                fire_at,
                ..
            } => {
                assert_eq!(workflow_id, "digest");
                assert_eq!(
                    schedule_id, "digest:0",
                    "id 缺省按 {{workflowId}}:{{index}} 推导"
                );
                let (_, _, _, h, m, _) = utc_parts(*fire_at);
                assert_eq!((h, m), (12, 30));
            }
            other => panic!("应为 Materialized，实际 {other:?}"),
        }

        // store 里能查到（说明 upsert 真的落下去且参数齐）
        let due = store
            .claim_due_schedule_buckets(ClaimDueScheduleBucketsArgs {
                now: T,
                limit: 10,
                lease_owner: "sweep".into(),
                lease_ms: 1000,
            })
            .unwrap();
        assert_eq!(due.len(), 1, "物化后 sweep 应能认领到桶");
        assert_eq!(due[0].schedule_id, "digest:0");
        assert_eq!(due[0].input, Some(serde_json::json!({ "batch": 100 })));
    }

    /// `enabled: false` → 登记为停用，不入 due。
    #[test]
    fn disabled_schedule_is_registered_but_not_due() {
        let (store, workflows) = store_and_workflows(vec![WorkflowScheduleDefinition {
            id: Some("off".into()),
            schedule: spec_interval(1000),
            overlap_policy: None,
            input: None,
            enabled: Some(false),
        }]);
        let results = materialize_workflow_schedules(
            store.as_ref(),
            &workflows,
            &MaterializeWorkflowSchedulesOptions {
                now: Some(T),
                cron_lookback_ms: None,
            },
        )
        .unwrap();
        assert!(matches!(
            results[0],
            MaterializedWorkflowSchedule::Disabled { .. }
        ));
        assert!(
            store
                .claim_due_schedule_buckets(ClaimDueScheduleBucketsArgs {
                    now: T,
                    limit: 10,
                    lease_owner: "sweep".into(),
                    lease_ms: 1000,
                })
                .unwrap()
                .is_empty(),
            "停用的 schedule 不该产生桶"
        );
    }

    /// 窗口内无匹配 → NotDue，且不写 store（不覆盖已有记录）。
    #[test]
    fn not_due_schedule_leaves_store_untouched() {
        let (store, workflows) = store_and_workflows(vec![WorkflowScheduleDefinition {
            id: Some("monthly".into()),
            schedule: spec_cron("0 0 1 * *"),
            overlap_policy: None,
            input: None,
            enabled: None,
        }]);
        let results = materialize_workflow_schedules(
            store.as_ref(),
            &workflows,
            &MaterializeWorkflowSchedulesOptions {
                now: Some(T),
                cron_lookback_ms: Some(24 * 60 * 60 * 1000), // 只回看 1 天
            },
        )
        .unwrap();
        assert!(matches!(
            results[0],
            MaterializedWorkflowSchedule::NotDue { .. }
        ));
        assert!(
            store
                .claim_due_schedule_buckets(ClaimDueScheduleBucketsArgs {
                    now: T,
                    limit: 10,
                    lease_owner: "sweep".into(),
                    lease_ms: 1000,
                })
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn negative_lookback_rejected() {
        let (store, workflows) = store_and_workflows(vec![]);
        assert!(
            materialize_workflow_schedules(
                store.as_ref(),
                &workflows,
                &MaterializeWorkflowSchedulesOptions {
                    now: Some(T),
                    cron_lookback_ms: Some(-1),
                },
            )
            .is_err()
        );
    }
}
