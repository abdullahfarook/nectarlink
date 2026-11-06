// SPDX-License-Identifier: GPL-3.0-or-later
//! `/v1/notifications/*` of the local automation API (see `docs/api.md`):
//! everything the Notifications section of the app does, for other local
//! tools: the live feed with its groups and Live Updates, replies and
//! actions, dismiss, clear all with Undo, history with search, and the
//! per-app rules.
//!
//! The rows come from the same builder as the app's own feed, so groups,
//! rules and counts always match what the window shows.

use std::{
    collections::HashMap,
    sync::{OnceLock, atomic::AtomicU64},
    time::Duration,
};

use nectarlink_core::{DeviceId, LinkState};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::{
    bridge::notifications::{Row, apps_json, build_rows, rule_name},
    core_host, icons,
    state::{AppRule, AppState, HistoryEntry},
};

type Reply = (u16, &'static str, Value);

/// The longest a feed request may wait for news.
const MAX_WAIT_SECS: u64 = 55;
/// The most history entries one request returns.
const MAX_HISTORY_LIMIT: usize = 300;

static REVISION: OnceLock<watch::Sender<u64>> = OnceLock::new();
static SUBSCRIBED: AtomicU64 = AtomicU64::new(0);

/// A counter that goes up with every change to notifications, history, apps or phones.
fn revision() -> &'static watch::Sender<u64> {
    let tx = REVISION.get_or_init(|| watch::channel(1).0);
    if SUBSCRIBED.swap(1, std::sync::atomic::Ordering::AcqRel) == 0 {
        let tx = tx.clone();
        #[cfg(not(test))]
        core_host::host().hub.subscribe(
            crate::state::Changes::NOTIFICATIONS
                | crate::state::Changes::HISTORY
                | crate::state::Changes::APPS
                | crate::state::Changes::DEVICES,
            move || {
                tx.send_modify(|v| *v += 1);
                true
            },
        );
        #[cfg(test)]
        drop(tx);
    }
    tx
}

fn ok() -> Reply {
    (200, "OK", json!({ "ok": true }))
}

fn bad(error: &str) -> Reply {
    (400, "Bad Request", json!({ "ok": false, "error": error }))
}

fn not_found(error: &str) -> Reply {
    (404, "Not Found", json!({ "ok": false, "error": error }))
}

fn parse<T: for<'de> Deserialize<'de> + Default>(body: &[u8]) -> Result<T, Reply> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(body).map_err(|_| bad("invalid_json"))
}

fn color(c: Option<u32>) -> String {
    c.map(|c| format!("#{:06X}", c & 0x00FF_FFFF)).unwrap_or_default()
}

/// One notification as the cards show it.
fn row_json(row: &Row) -> Value {
    let n = &row.view.notification;
    let reply = n.actions.iter().find(|a| a.reply);
    let live = n.live.as_ref().map(|live| {
        json!({
            "progress": live.progress.unwrap_or(0),
            "max": live.max.unwrap_or(0),
            "indeterminate": live.indeterminate,
            "chip": live.chip.as_deref().unwrap_or_default(),
            "segments": live.segments.iter().map(|s| json!({ "length": s.length, "color": color(s.color) })).collect::<Vec<_>>(),
            "points": live.points.iter().map(|p| json!({ "position": p.position, "color": color(p.color) })).collect::<Vec<_>>(),
            "chronometer": live.chronometer,
            "countdown": live.countdown,
            "when": n.when,
        })
    });
    json!({
        "deviceId": row.view.device.to_string(),
        "deviceName": row.device_name,
        "key": n.key,
        "app": n.app,
        "appName": n.app_name,
        "title": n.title.as_deref().unwrap_or_default(),
        "text": n.text.as_deref().unwrap_or_default(),
        "sub": n.sub.as_deref().unwrap_or_default(),
        "when": n.when,
        "iconUrl": row.icon_url,
        "imageUrl": row.image_url,
        // Buttons other than the reply: [{ id, title }].
        "actions": n.actions.iter().filter(|a| !a.reply).map(|a| json!({ "id": a.id, "title": a.title })).collect::<Vec<_>>(),
        "replyAction": reply.map_or("", |a| a.id.as_str()),
        "replyLabel": reply.map_or("", |a| a.title.as_str()),
        "silent": n.silent,
        "ongoing": n.ongoing,
        // Replies sent from this PC: [{ text, pending }].
        "replies": row.replies.iter().map(|r| json!({ "text": r.text, "pending": r.pending })).collect::<Vec<_>>(),
        "otpCode": crate::notifications::notification_otp(n).unwrap_or_default(),
        "live": live,
        "conversation": n.conversation.as_ref().and_then(|c| serde_json::to_value(c).ok()),
        "groupCount": row.group_count,
        "groupIndex": row.group_index,
        "groupExpanded": row.group_expanded,
        "groupSummary": row.group_summary,
    })
}

/// The feed: rows, the count the app shows, Undo and the history switch.
fn feed(device: Option<DeviceId>) -> Value {
    let (rows, undo, history, online, history_count, can_open_app) =
        core_host::host().hub.read(|s: &AppState| {
            let online: HashMap<String, bool> = s
                .devices
                .iter()
                .map(|d| (d.id.to_string(), matches!(d.link, LinkState::Online { .. })))
                .collect();
            let history_count =
                s.history.iter().filter(|h| s.app_rule(&h.notification.app) != AppRule::Hidden).count();
            // Clicking a card opens the phone's app in a window of its own, when the app says that works for this phone.
            let can_open_app = s.devices.iter().filter(|d| device.is_none_or(|id| d.id == id)).any(|d| {
                matches!(
                    s.matrices.get(&d.id).and_then(|m| m.state("mirroring.app_windows")),
                    Some(nectarlink_core::FeatureState::Available)
                )
            });
            (
                build_rows(s),
                s.last_dismissed_label.clone(),
                s.history_enabled,
                online,
                history_count,
                can_open_app,
            )
        });
    let rows: Vec<&Row> = rows.iter().filter(|r| device.is_none_or(|d| r.view.device == d)).collect();
    // Like the app: each group counts once for all its members.
    let count: i32 = rows.iter().filter(|r| r.group_index == 0).map(|r| r.group_count).sum();
    json!({
        "ok": true,
        "rev": *revision().borrow(),
        "count": count,
        "undoLabel": undo,
        "historyEnabled": history,
        "historyCount": history_count,
        "canOpenApp": can_open_app,
        "online": online,
        "items": rows.iter().map(|r| row_json(r)).collect::<Vec<_>>(),
    })
}

fn device_from(query: &HashMap<String, String>) -> Option<DeviceId> {
    query.get("device").and_then(|d| d.parse().ok())
}

/// The phone to act on: the one asked for, else the one that owns `key`,
/// else the first online one.
fn target(requested: Option<&str>, key: Option<&str>) -> Option<String> {
    if let Some(id) = requested.map(str::trim).filter(|s| !s.is_empty()) {
        return id.parse::<DeviceId>().ok().map(|d| d.to_string());
    }
    core_host::host().hub.read(|s| {
        if let Some(key) = key
            && let Some(v) = s.notifications.iter().find(|n| n.notification.key == key)
        {
            return Some(v.device.to_string());
        }
        s.devices
            .iter()
            .find(|d| matches!(d.link, LinkState::Online { .. }))
            .or_else(|| s.devices.first())
            .map(|d| d.id.to_string())
    })
}

fn history_json(entry: &HistoryEntry, s: &AppState) -> Value {
    let n = &entry.notification;
    json!({
        "deviceId": entry.device.to_string(),
        "deviceName": s.name_of(&entry.device).unwrap_or_default(),
        "key": n.key,
        "app": n.app,
        "appName": n.app_name,
        "title": n.title.as_deref().unwrap_or_default(),
        "text": n.text.as_deref().unwrap_or_default(),
        "sub": n.sub.as_deref().unwrap_or_default(),
        "when": n.when,
        "removedAt": entry.removed_at,
        "iconUrl": s.app_icons.get(&n.app).map(|p| icons::file_url(p)).unwrap_or_default(),
    })
}

fn matches_query(entry: &HistoryEntry, query: &str) -> bool {
    let n = &entry.notification;
    [Some(n.app_name.as_str()), n.title.as_deref(), n.text.as_deref(), n.sub.as_deref()]
        .into_iter()
        .flatten()
        .any(|field| field.to_lowercase().contains(query))
}

fn history(query: &HashMap<String, String>) -> Value {
    let q = query.get("q").map(|q| q.trim().to_lowercase()).unwrap_or_default();
    let limit = query
        .get("limit")
        .and_then(|l| l.parse::<usize>().ok())
        .unwrap_or(MAX_HISTORY_LIMIT)
        .clamp(1, MAX_HISTORY_LIMIT);
    let device = device_from(query);
    core_host::host().hub.read(|s| {
        let total = s.history.len();
        let items: Vec<Value> = s
            .history
            .iter()
            .filter(|h| s.app_rule(&h.notification.app) != AppRule::Hidden)
            .filter(|h| device.is_none_or(|d| h.device == d))
            .filter(|h| q.is_empty() || matches_query(h, &q))
            .take(limit)
            .map(|h| history_json(h, s))
            .collect();
        json!({
            "ok": true,
            "rev": *revision().borrow(),
            "enabled": s.history_enabled,
            "total": total,
            "items": items,
        })
    })
}

fn rules() -> Value {
    core_host::host().hub.read(|s| {
        json!({
            "ok": true,
            "historyEnabled": s.history_enabled,
            "apps": serde_json::from_str::<Value>(&apps_json(s)).unwrap_or_else(|_| json!([])),
        })
    })
}

/// What a tool needs to paint like the app: the theme settings and the color schemes the app derives
/// from the wallpaper and from paired phones (the same JSON its own Theme reads).
async fn theme() -> Value {
    #[cfg(not(test))]
    {
        let settings = crate::settings::Settings::load(&core_host::host().data_dir);
        let wallpaper = tokio::task::spawn_blocking(|| {
            crate::win::wallpaper::seed().map(|seed| crate::palette::scheme_json(seed).to_string())
        })
        .await
        .ok()
        .flatten();
        let phones = core_host::host().hub.read(|s| crate::bridge::app::phone_colors_json(&s.devices));
        json!({
            "ok": true,
            "theme": serde_json::to_value(settings.theme).unwrap_or(Value::Null),
            "seed": settings.seed,
            "colorMode": serde_json::to_value(settings.color_mode).unwrap_or(Value::Null),
            "amoled": settings.amoled,
            "systemDark": crate::win::system_dark(),
            "wallpaperColors": wallpaper.unwrap_or_default(),
            "phoneColors": phones,
        })
    }
    #[cfg(test)]
    {
        json!({ "ok": true })
    }
}

#[derive(Debug, Default, Deserialize)]
struct ActionPayload {
    #[serde(default)]
    device: Option<String>,
    #[serde(default)]
    key: String,
    #[serde(default)]
    action: String,
    #[serde(default)]
    reply: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct DismissPayload {
    #[serde(default)]
    device: Option<String>,
    #[serde(default)]
    key: String,
    #[serde(default)]
    app: String,
}

#[derive(Debug, Default, Deserialize)]
struct RulePayload {
    #[serde(default)]
    app: Option<String>,
    #[serde(default)]
    rule: Option<String>,
    #[serde(default, rename = "historyEnabled")]
    history_enabled: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
struct CodePayload {
    #[serde(default)]
    code: String,
}

#[derive(Debug, Default, Deserialize)]
struct OpenAppPayload {
    #[serde(default)]
    device: Option<String>,
    #[serde(default)]
    app: String,
    #[serde(default, rename = "appName")]
    app_name: Option<String>,
}

/// Waits (up to `wait` seconds) until the revision moves past `since`.
async fn wait_for_news(since: u64, wait: u64) {
    let mut rx = revision().subscribe();
    let _ =
        tokio::time::timeout(Duration::from_secs(wait.min(MAX_WAIT_SECS)), rx.wait_for(|rev| *rev != since))
            .await;
}

/// Handles a `/v1/notifications...` request, or `None` if the path isn't one of ours.
pub async fn route(method: &str, path: &str, query: &HashMap<String, String>, body: &[u8]) -> Option<Reply> {
    let sub = path.strip_prefix("/v1/notifications")?;
    let sub = sub.trim_end_matches('/');
    Some(match (method, sub) {
        ("GET", "") => {
            if let (Some(since), Some(wait)) = (
                query.get("since").and_then(|v| v.parse::<u64>().ok()),
                query.get("wait").and_then(|v| v.parse::<u64>().ok()),
            ) && wait > 0
                && since == *revision().borrow()
            {
                wait_for_news(since, wait).await;
            }
            (200, "OK", feed(device_from(query)))
        }
        ("GET", "/history") => (200, "OK", history(query)),
        ("GET", "/rules") => (200, "OK", rules()),
        ("GET", "/theme") => (200, "OK", theme().await),
        ("POST", "/action") => match parse::<ActionPayload>(body) {
            Ok(p) => action(p),
            Err(e) => e,
        },
        ("POST", "/dismiss") => match parse::<DismissPayload>(body) {
            Ok(p) if p.key.is_empty() => bad("missing_key"),
            Ok(p) => match target(p.device.as_deref(), Some(&p.key)) {
                Some(device) => {
                    crate::notifications::dismiss_with_undo(&device, p.key);
                    ok()
                }
                None => not_found("no_device"),
            },
            Err(e) => e,
        },
        ("POST", "/dismiss-group") => match parse::<DismissPayload>(body) {
            Ok(p) if p.app.is_empty() => bad("missing_app"),
            Ok(p) => match target(p.device.as_deref(), None) {
                Some(device) => {
                    crate::notifications::dismiss_app_group(&device, &p.app);
                    ok()
                }
                None => not_found("no_device"),
            },
            Err(e) => e,
        },
        ("POST", "/clear") => match parse::<DismissPayload>(body) {
            Ok(p) => match target(p.device.as_deref(), None) {
                Some(device) => {
                    crate::notifications::dismiss_all_for(&device);
                    ok()
                }
                None => not_found("no_device"),
            },
            Err(e) => e,
        },
        ("POST", "/undo") => {
            core_host::host().hub.update(AppState::undo_dismiss);
            ok()
        }
        ("POST", "/undo/clear") => {
            core_host::host().hub.update(AppState::clear_undo);
            ok()
        }
        ("POST", "/toggle-group") => match parse::<DismissPayload>(body) {
            Ok(p) if p.app.is_empty() => bad("missing_app"),
            Ok(p) => {
                core_host::host().hub.update(|s| s.toggle_app_expanded(&p.app));
                ok()
            }
            Err(e) => e,
        },
        ("POST", "/history/clear") => {
            core_host::host().hub.update(AppState::clear_history);
            ok()
        }
        ("POST", "/rules") => match parse::<RulePayload>(body) {
            Ok(p) => rule(p),
            Err(e) => e,
        },
        ("POST", "/copy-code") => match parse::<CodePayload>(body) {
            Ok(p) if p.code.trim().is_empty() => bad("missing_code"),
            Ok(p) => {
                crate::notifications::copy_otp(&p.code);
                ok()
            }
            Err(e) => e,
        },
        ("POST", "/mirror") => match parse::<OpenAppPayload>(body) {
            Ok(p) => mirror_screen(p.device.as_deref()),
            Err(e) => e,
        },
        ("POST", "/open-app") => match parse::<OpenAppPayload>(body) {
            Ok(p) => open_app(p),
            Err(e) => e,
        },
        _ => not_found("not_found"),
    })
}

fn action(p: ActionPayload) -> Reply {
    if p.key.is_empty() {
        return bad("missing_key");
    }
    if p.action.is_empty() {
        return bad("missing_action");
    }
    let Some(device) = target(p.device.as_deref(), Some(&p.key)) else { return not_found("no_device") };
    let reply = match p.reply {
        Some(text) => {
            let text = text.trim().to_owned();
            if text.is_empty() {
                return bad("empty_reply");
            }
            Some(text)
        }
        None => None,
    };
    crate::notifications::run_action(&device, p.key, p.action, reply);
    ok()
}

fn rule(p: RulePayload) -> Reply {
    if p.app.is_none() && p.history_enabled.is_none() {
        return bad("nothing_to_change");
    }
    if let Some(app) = p.app.as_deref() {
        let rule = match p.rule.as_deref() {
            Some("show") => AppRule::Show,
            Some("quiet") => AppRule::Quiet,
            Some("hidden") => AppRule::Hidden,
            _ => return bad("invalid_rule"),
        };
        crate::notifications::set_app_rule(app, rule);
        debug_assert!(!rule_name(rule).is_empty());
    }
    if let Some(on) = p.history_enabled {
        core_host::host().hub.update(|s| s.set_history_enabled(on));
    }
    ok()
}

/// Asks the phone for its screen (the "Mirror screen" tile of the app's Home page).
fn mirror_screen(device: Option<&str>) -> Reply {
    let Some(device) = target(device, None).and_then(|d| d.parse::<DeviceId>().ok()) else {
        return not_found("no_device");
    };
    #[cfg(not(test))]
    crate::mirror::start(device);
    #[cfg(test)]
    let _ = device;
    ok()
}

fn open_app(p: OpenAppPayload) -> Reply {
    if !nectarlink_core::is_package_name(&p.app) {
        return bad("invalid_app");
    }
    let Some(device) = target(p.device.as_deref(), None).and_then(|d| d.parse::<DeviceId>().ok()) else {
        return not_found("no_device");
    };
    let label = p.app_name.filter(|n| !n.is_empty()).unwrap_or_else(|| p.app.clone());
    crate::mirror::start_app(device, p.app, label);
    ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_paths_are_not_ours() {
        let q = HashMap::new();
        assert!(route("GET", "/v1/devices", &q, b"").await.is_none());
        let reply = route("GET", "/v1/notifications/nope", &q, b"").await.unwrap();
        assert_eq!(reply.0, 404);
    }

    #[tokio::test]
    async fn bad_requests_are_refused_before_touching_the_app() {
        let q = HashMap::new();
        let cases: &[(&str, &str, &[u8], &str)] = &[
            ("POST", "/v1/notifications/action", b"{not json", "invalid_json"),
            ("POST", "/v1/notifications/action", br#"{"action":"a"}"#, "missing_key"),
            ("POST", "/v1/notifications/action", br#"{"key":"k"}"#, "missing_action"),
            ("POST", "/v1/notifications/dismiss", b"{}", "missing_key"),
            ("POST", "/v1/notifications/dismiss-group", b"{}", "missing_app"),
            ("POST", "/v1/notifications/toggle-group", b"{}", "missing_app"),
            ("POST", "/v1/notifications/copy-code", b"{}", "missing_code"),
            ("POST", "/v1/notifications/rules", b"{}", "nothing_to_change"),
            ("POST", "/v1/notifications/rules", br#"{"app":"com.x","rule":"loud"}"#, "invalid_rule"),
            ("POST", "/v1/notifications/open-app", br#"{"app":"not a package"}"#, "invalid_app"),
        ];
        for (method, path, body, error) in cases {
            let (status, _, value) = route(method, path, &q, body).await.unwrap();
            assert_eq!(status, 400, "{path}");
            assert_eq!(value["error"], *error, "{path}");
        }
    }

    #[test]
    fn history_search_matches_any_field_case_insensitively() {
        use nectarlink_core::Notification;
        let n = Notification {
            key: "k".into(),
            app: "com.whatsapp".into(),
            app_name: "WhatsApp".into(),
            title: Some("Dure Kafian".into()),
            text: Some("Assalamualaikum".into()),
            sub: None,
            when: 1,
            actions: Vec::new(),
            silent: false,
            ongoing: false,
            icon: None,
            image: None,
            live: None,
            conversation: None,
        };
        let entry = HistoryEntry { device: DeviceId([7u8; 32]), notification: n, removed_at: 2 };
        assert!(matches_query(&entry, "dure"));
        assert!(matches_query(&entry, "whatsapp"));
        assert!(matches_query(&entry, "salam"));
        assert!(!matches_query(&entry, "telegram"));
    }
}
