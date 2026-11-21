mod identity;
mod parallel_click;
mod parallel_dedup;
mod parse;
use hashbrown::HashMap;
use identity::People;
use parquet::{
    basic::Compression,
    data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};
use rayon::prelude::*;
use std::{
    cmp::Ordering,
    env, fs,
    path::{Path, PathBuf},
    sync::Arc,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
fn files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut v = vec![];
    for e in fs::read_dir(dir)? {
        let p = e?.path();
        if p.extension().is_some_and(|x| x == "csv") && p.is_file() {
            v.push(p)
        }
    }
    v.sort();
    Ok(v)
}
fn rows<const N: usize>(p: &Path, mut f: impl FnMut([&[u8]; N])) -> Result<()> {
    let b = fs::read(p)?;
    for line in b.split(|x| *x == b'\n').skip(1) {
        if let Some(r) = parse::fields::<N>(line) {
            f(r)
        }
    }
    Ok(())
}
fn s(x: &[u8]) -> String {
    String::from_utf8_lossy(x).into_owned()
}
#[inline]
fn product_number(id: &[u8]) -> Option<usize> {
    if id.len() < 2 || id[0] != b'p' || (id.len() > 2 && id[1] == b'0') {
        return None;
    }
    id[1..].iter().try_fold(0usize, |number, &byte| {
        byte.is_ascii_digit()
            .then(|| {
                number
                    .checked_mul(10)?
                    .checked_add(usize::from(byte - b'0'))
            })
            .flatten()
    })
}
#[inline]
pub(crate) fn campaign_number(id: &[u8]) -> Option<usize> {
    if id.len() < 3 || id[0] != b'c' || id[1] != b'_' || (id.len() > 3 && id[2] == b'0') {
        return None;
    }
    id[2..].iter().try_fold(0usize, |number, &byte| {
        byte.is_ascii_digit()
            .then(|| {
                number
                    .checked_mul(10)?
                    .checked_add(usize::from(byte - b'0'))
            })
            .flatten()
    })
}
pub struct Candidate {
    ingest: i64,
    file: usize,
    offset: usize,
    len: usize,
}
/// A compact, exact representation for the generator's `e`/`E` plus lowercase
/// base-36 event IDs. Other IDs remain borrowed byte slices, so this never
/// changes the equality rule for an input outside that common shape.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum EventKey<'a> {
    Packed(u64),
    Bytes(&'a [u8]),
}
impl<'a> EventKey<'a> {
    pub fn from_bytes(id: &'a [u8]) -> Self {
        if id.len() >= 2 && id.len() <= 12 && matches!(id[0], b'e' | b'E') {
            let mut value = 1u64;
            for &byte in &id[1..] {
                let digit = match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'z' => byte - b'a' + 10,
                    _ => return Self::Bytes(id),
                };
                let Some(next) = value
                    .checked_mul(37)
                    .and_then(|value| value.checked_add(u64::from(digit) + 1))
                else {
                    return Self::Bytes(id);
                };
                value = next;
            }
            return Self::Packed(value | (u64::from(id[0] == b'E') << 63));
        }
        Self::Bytes(id)
    }
}
struct Event {
    person: usize,
    ts: i64,
    price: i64,
    category: usize,
}
struct PendingEvent<'a> {
    user: &'a str,
    ts: i64,
    price: i64,
    category: usize,
}
struct Click {
    person: usize,
    ts: i64,
    campaign: usize,
    id: ClickId,
}
/// The normal `k` plus base-36 click IDs fit into nine 6-bit bytes. Padding
/// with zero makes unsigned integer comparison exactly match bytewise order.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ClickId {
    Packed(u64),
    Bytes(String),
}
impl ClickId {
    pub(crate) fn from_bytes(id: &[u8]) -> Self {
        if id.len() >= 2 && id.len() <= 10 && id[0] == b'k' {
            let mut key = 0u64;
            for &byte in &id[1..] {
                let code = match byte {
                    b'0'..=b'9' => byte - b'0' + 1,
                    b'a'..=b'z' => byte - b'a' + 11,
                    _ => return Self::Bytes(s(id)),
                };
                key = (key << 6) | u64::from(code);
            }
            return Self::Packed(key << (6 * (10 - id.len())));
        }
        Self::Bytes(s(id))
    }

    fn cmp_bytes(key: u64, other: &[u8]) -> Ordering {
        match b'k'.cmp(other.first().unwrap_or(&0)) {
            Ordering::Equal => {}
            order => return order,
        }
        for at in 0..9 {
            let code = ((key >> (6 * (8 - at))) & 63) as u8;
            let byte = match code {
                0 => return (at + 1).cmp(&other.len()),
                1..=10 => b'0' + code - 1,
                _ => b'a' + code - 11,
            };
            match byte.cmp(other.get(at + 1).unwrap_or(&0)) {
                Ordering::Equal => {}
                order => return order,
            }
        }
        10usize.cmp(&other.len())
    }
}
impl Ord for ClickId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Packed(a), Self::Packed(b)) => a.cmp(b),
            (Self::Bytes(a), Self::Bytes(b)) => a.cmp(b),
            (Self::Packed(a), Self::Bytes(b)) => Self::cmp_bytes(*a, b.as_bytes()),
            (Self::Bytes(a), Self::Packed(b)) => Self::cmp_bytes(*b, a.as_bytes()).reverse(),
        }
    }
}
impl PartialOrd for ClickId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
pub(crate) struct PendingClick {
    pub(crate) user: String,
    pub(crate) ts: i64,
    pub(crate) campaign: usize,
}
struct MissingUsers<'a> {
    packed: HashMap<u64, &'a str>,
    other: Vec<&'a str>,
}
#[derive(Default)]
struct UserDay {
    sessions: i64,
    events: i64,
    revenue: i64,
    categories: Vec<(usize, i64)>,
}
#[derive(Default)]
struct CampDay {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assists: i64,
}
enum Col {
    Str(Vec<String>),
    Opt(Vec<Option<String>>),
    I64(Vec<i64>),
    Date(Vec<i32>),
}
#[inline]
fn add_amount(values: &mut Vec<(usize, i64)>, key: usize, amount: i64) {
    if let Some((_, value)) = values.iter_mut().find(|(existing, _)| *existing == key) {
        *value += amount;
    } else {
        values.push((key, amount));
    }
}

#[inline]
fn add_count(values: &mut Vec<(usize, usize)>, key: usize, amount: usize) {
    if let Some((_, value)) = values.iter_mut().find(|(existing, _)| *existing == key) {
        *value += amount;
    } else {
        values.push((key, amount));
    }
}

#[inline]
fn remove_count(values: &mut Vec<(usize, usize)>, key: usize) {
    let index = values
        .iter()
        .position(|(existing, _)| *existing == key)
        .expect("an active click has a campaign count");
    values[index].1 -= 1;
    if values[index].1 == 0 {
        values.swap_remove(index);
    }
}

fn intern_event_users(people: &mut People, pending: &[PendingEvent<'_>]) {
    let missing: Vec<_> = pending
        .par_chunks(65_536)
        .map(|rows| {
            let mut missing = MissingUsers {
                packed: HashMap::new(),
                other: vec![],
            };
            for row in rows {
                if people.resolve_known(row.user).is_some() {
                    continue;
                }
                if let Some(key) = identity::packed_id(row.user.as_bytes()) {
                    missing.packed.entry(key).or_insert(row.user);
                } else {
                    missing.other.push(row.user);
                }
            }
            missing
        })
        .collect();
    for missing in missing {
        for (_, user) in missing.packed {
            people.intern(user);
        }
        for user in missing.other {
            people.intern(user);
        }
    }
}

fn intern_click_users(people: &mut People, pending: &[(ClickId, PendingClick)]) {
    let missing: Vec<_> = pending
        .par_chunks(65_536)
        .map(|rows| {
            let mut missing = MissingUsers {
                packed: HashMap::new(),
                other: vec![],
            };
            for (_, row) in rows {
                if people.resolve_known(&row.user).is_some() {
                    continue;
                }
                if let Some(key) = identity::packed_id(row.user.as_bytes()) {
                    missing.packed.entry(key).or_insert(&row.user);
                } else {
                    missing.other.push(&row.user);
                }
            }
            missing
        })
        .collect();
    for missing in missing {
        for (_, user) in missing.packed {
            people.intern(user);
        }
        for user in missing.other {
            people.intern(user);
        }
    }
}

fn output(path: &Path, schema: &str, columns: Vec<Col>) -> Result<()> {
    fs::create_dir_all(path)?;
    let file = fs::File::create(path.join("part-0.parquet"))?;
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build(),
    );
    let mut writer = SerializedFileWriter::new(file, Arc::new(parse_message_type(schema)?), props)?;
    let mut group = writer.next_row_group()?;
    for col in columns {
        let mut w = group.next_column()?.ok_or("missing column")?;
        match col {
            Col::Str(v) => {
                let vals: Vec<ByteArray> = v.iter().map(|x| ByteArray::from(x.as_str())).collect();
                w.typed::<ByteArrayType>().write_batch(&vals, None, None)?;
            }
            Col::Opt(v) => {
                let defs: Vec<i16> = v.iter().map(|x| i16::from(x.is_some())).collect();
                let vals: Vec<ByteArray> = v
                    .iter()
                    .filter_map(|x| x.as_ref())
                    .map(|x| ByteArray::from(x.as_str()))
                    .collect();
                w.typed::<ByteArrayType>()
                    .write_batch(&vals, Some(&defs), None)?;
            }
            Col::I64(v) => {
                w.typed::<Int64Type>().write_batch(&v, None, None)?;
            }
            Col::Date(v) => {
                w.typed::<Int32Type>().write_batch(&v, None, None)?;
            }
        }
        w.close()?;
    }
    group.close()?;
    writer.close()?;
    Ok(())
}
fn run() -> Result<()> {
    let metrics = env::var_os("SHOPPING_METRICS").is_some();
    let mut mark = std::time::Instant::now();
    macro_rules! stage {
        ($name:literal) => {
            if metrics {
                eprintln!("{}: {:.3}s", $name, mark.elapsed().as_secs_f64());
            }
            mark = std::time::Instant::now();
        };
    }
    let args: Vec<_> = env::args_os().collect();
    let data = PathBuf::from(
        args.get(1)
            .cloned()
            .unwrap_or_else(|| env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into())),
    );
    let out = PathBuf::from(
        args.get(2)
            .cloned()
            .unwrap_or_else(|| env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into())),
    );
    let m: serde_json::Value = serde_json::from_slice(&fs::read(data.join("manifest.json"))?)?;
    let start = parse::timestamp(
        m["window_start"]
            .as_str()
            .ok_or("missing window_start")?
            .as_bytes(),
    )
    .ok_or("invalid window_start")?;
    let end = parse::timestamp(
        m["window_end"]
            .as_str()
            .ok_or("missing window_end")?
            .as_bytes(),
    )
    .ok_or("invalid window_end")?;
    let mut people = People::default();
    for p in files(&data.join("links"))? {
        rows::<2>(&p, |r| {
            if !r[0].is_empty() && !r[1].is_empty() {
                people.link(&s(r[0]), &s(r[1]))
            }
        })?;
    }
    let mut categories = vec!["unknown".to_string()];
    let mut cat_ids = HashMap::new();
    cat_ids.insert("unknown".to_string(), 0usize);
    let mut products = HashMap::new();
    let mut product_categories = vec![0usize];
    rows::<2>(&data.join("products.csv"), |r| {
        let c = s(r[1]);
        let id = *cat_ids.entry(c.clone()).or_insert_with(|| {
            categories.push(c);
            categories.len() - 1
        });
        if let Some(number) = product_number(r[0]) {
            if product_categories.len() <= number {
                product_categories.resize(number + 1, 0);
            }
            product_categories[number] = id;
        }
        products.insert(s(r[0]), id);
    })?;
    let mut campaigns = vec!["organic".to_string()];
    let mut channels = vec!["organic".to_string()];
    let mut camp_ids = HashMap::new();
    let mut campaign_ids = vec![0usize];
    rows::<2>(&data.join("campaigns.csv"), |r| {
        let id = campaigns.len();
        campaigns.push(s(r[0]));
        channels.push(s(r[1]));
        if let Some(number) = campaign_number(r[0]) {
            if campaign_ids.len() <= number {
                campaign_ids.resize(number + 1, 0);
            }
            campaign_ids[number] = id;
        }
        camp_ids.insert(s(r[0]), id);
    })?;
    stage!("catalogs and links");
    let event_files = files(&data.join("events"))?;
    let mut maps = Vec::with_capacity(event_files.len());
    for path in &event_files {
        let file = fs::File::open(path)?;
        if file.metadata()?.len() == 0 {
            maps.push(None)
        } else {
            maps.push(Some(unsafe { memmap2::Mmap::map(&file)? }))
        }
    }
    let mut candidates: HashMap<EventKey<'_>, Candidate> = HashMap::new();
    if env::var_os("SHOPPING_SERIAL_DEDUP").is_some() {
        for (file, map) in maps.iter().enumerate() {
            let Some(map) = map else { continue };
            let mut offset = 0usize;
            for (line_no, line) in map.split(|x| *x == b'\n').enumerate() {
                let row_offset = offset;
                offset += line.len() + 1;
                if line_no == 0 {
                    continue;
                }
                let Some(r) = parse::fields::<7>(line) else {
                    continue;
                };
                if r[0].is_empty() {
                    continue;
                }
                let Some(ingest) = parse::timestamp(r[3]) else {
                    continue;
                };
                let id = EventKey::from_bytes(r[0]);
                if candidates.get(&id).is_some_and(|x| x.ingest <= ingest) {
                    continue;
                }
                candidates.insert(
                    id,
                    Candidate {
                        ingest,
                        file,
                        offset: row_offset,
                        len: line.len(),
                    },
                );
            }
        }
    }
    let mut tables = vec![candidates];
    if env::var_os("SHOPPING_SERIAL_DEDUP").is_none() {
        tables = parallel_dedup::reduce(&maps);
    }
    stage!("event deduplication");
    let candidates = tables.into_iter().flat_map(|table| table.into_values());
    let mut candidates: Vec<_> = candidates.collect();
    if env::var_os("SHOPPING_UNORDERED_ROWS").is_none() {
        candidates.par_sort_unstable_by_key(|e| (e.file, e.offset));
    }
    stage!("event candidate ordering");
    let pending: std::result::Result<Vec<PendingEvent<'_>>, std::str::Utf8Error> = candidates
        .par_iter()
        .map(|e| {
            let map = maps[e.file].as_ref().unwrap();
            let r = parse::fields::<7>(&map[e.offset..e.offset + e.len]).unwrap();
            let Some(ts) = parse::timestamp(r[2]) else {
                return Ok(None);
            };
            let kind = match r[4] {
                b"view" => 1,
                b"cart" => 2,
                b"purchase" => 3,
                _ => 0,
            };
            let price = if kind == 3 {
                std::str::from_utf8(r[6])
                    .ok()
                    .filter(|x| !x.is_empty() && x.bytes().all(|b| b.is_ascii_digit()))
                    .and_then(|x| x.parse::<i64>().ok())
                    .filter(|x| (1..=10_000_000).contains(x))
            } else {
                Some(0)
            };
            if r[1].is_empty()
                || r[5].is_empty()
                || kind == 0
                || ts < start
                || ts >= end
                || e.ingest - ts > DAY
                || price.is_none()
            {
                return Ok(None);
            }
            let category =
                match product_number(r[5]).and_then(|number| product_categories.get(number)) {
                    Some(&category) => category,
                    None => *products.get(std::str::from_utf8(r[5])?).unwrap_or(&0),
                };
            Ok(Some(PendingEvent {
                user: std::str::from_utf8(r[1])?,
                ts,
                price: price.unwrap(),
                category,
            }))
        })
        .filter_map(std::result::Result::transpose)
        .collect();
    let pending = pending?;
    stage!("event validation");
    intern_event_users(&mut people, &pending);
    stage!("event identity interning");
    let mut events: Vec<_> = pending
        .par_iter()
        .map(|e| Event {
            person: people
                .resolve_known(e.user)
                .expect("all pending users were interned"),
            ts: e.ts,
            price: e.price,
            category: e.category,
        })
        .collect();
    drop(pending);
    stage!("event identity resolution");
    drop(candidates);
    drop(maps);
    if end - start <= i64::from(u32::MAX) && people.len() <= u32::MAX as usize {
        events.par_sort_unstable_by_key(|e| ((e.person as u64) << 32) | (e.ts - start) as u64);
    } else {
        events.par_sort_unstable_by_key(|e| (e.person, e.ts));
    }
    stage!("event sorting");
    let click_files = files(&data.join("clicks"))?;
    let mut click_maps = Vec::with_capacity(click_files.len());
    for path in &click_files {
        let file = fs::File::open(path)?;
        if file.metadata()?.len() == 0 {
            click_maps.push(None)
        } else {
            click_maps.push(Some(unsafe { memmap2::Mmap::map(&file)? }))
        }
    }
    let tables = parallel_click::reduce(&click_maps, start - WEEK, end, &camp_ids, &campaign_ids);
    stage!("click parsing and deduplication");
    let pending: Vec<_> = tables
        .into_iter()
        .flat_map(|table| table.into_iter())
        .collect();
    intern_click_users(&mut people, &pending);
    stage!("click identity interning");
    let mut clicks: Vec<Click> = pending
        .into_par_iter()
        .map(|(id, click)| Click {
            person: people
                .resolve_known(&click.user)
                .expect("all pending users were interned"),
            ts: click.ts,
            campaign: click.campaign,
            id,
        })
        .collect();
    stage!("click identity resolution");
    clicks.par_sort_unstable_by(|a, b| (a.person, a.ts, &a.id).cmp(&(b.person, b.ts, &b.id)));
    stage!("click sorting");
    let mut users = vec![];
    let mut camps: HashMap<(usize, i32), CampDay> = HashMap::new();
    let (mut ei, mut ci) = (0, 0);
    while ei < events.len() {
        let person = events[ei].person;
        while ci < clicks.len() && clicks[ci].person < person {
            ci += 1
        }
        let begin = ci;
        while ci < clicks.len() && clicks[ci].person == person {
            ci += 1
        }
        let clicks = &clicks[begin..ci];
        let mut add = 0;
        let mut active_start = 0;
        let mut counts = vec![];
        let mut current_day = None;
        let mut daily = UserDay::default();
        while ei < events.len() && events[ei].person == person {
            let st = events[ei].ts;
            let day = st.div_euclid(DAY) as i32;
            let (mut last, mut count, mut revenue) = (st, 0, 0);
            let mut cats = vec![];
            while ei < events.len()
                && events[ei].person == person
                && events[ei].ts - last <= 1_800_000
            {
                let e = &events[ei];
                last = e.ts;
                count += 1;
                revenue += e.price;
                if e.price > 0 {
                    add_amount(&mut cats, e.category, e.price)
                }
                ei += 1
            }
            if current_day != Some(day) {
                if let Some(previous) = current_day {
                    users.push(((person, previous), std::mem::take(&mut daily)));
                }
                current_day = Some(day);
            }
            daily.sessions += 1;
            daily.events += count;
            daily.revenue += revenue;
            for (k, v) in cats {
                add_amount(&mut daily.categories, k, v)
            }
            while add < clicks.len() && clicks[add].ts <= st {
                add_count(&mut counts, clicks[add].campaign, 1);
                add += 1
            }
            while active_start < add && clicks[active_start].ts < st - WEEK {
                remove_count(&mut counts, clicks[active_start].campaign);
                active_start += 1;
            }
            let credited = if active_start < add {
                let ts = clicks[add - 1].ts;
                let j = clicks.partition_point(|c| c.ts < ts);
                clicks[j].campaign
            } else {
                0
            };
            let c = camps.entry((credited, day)).or_default();
            c.sessions += 1;
            c.revenue += revenue;
            c.purchasing += i64::from(revenue > 0);
            for &(campaign, _) in &counts {
                camps.entry((campaign, day)).or_default().assists += 1
            }
        }
        if let Some(day) = current_day {
            users.push(((person, day), daily));
        }
    }
    stage!("sessionization and attribution");
    let (mut un, mut ud, mut us, mut ue, mut ur, mut ut) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    for ((person, day), v) in users {
        un.push(people.canonical(person).to_string());
        ud.push(day);
        us.push(v.sessions);
        ue.push(v.events);
        ur.push(v.revenue);
        ut.push(
            v.categories
                .iter()
                .min_by(|(a, va), (b, vb)| {
                    vb.cmp(va).then_with(|| categories[*a].cmp(&categories[*b]))
                })
                .map(|(c, _)| categories[*c].clone()),
        );
    }
    output(&out.join("user_daily"),"message schema { REQUIRED BINARY user_id (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BINARY top_category (UTF8); }",vec![Col::Str(un),Col::Date(ud),Col::I64(us),Col::I64(ue),Col::I64(ur),Col::Opt(ut)])?;
    let (mut cn, mut ch, mut cd, mut cs, mut cp, mut cr, mut ca) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
    let mut camps: Vec<_> = camps.into_iter().collect();
    camps.par_sort_unstable_by_key(|((campaign, day), _)| (*campaign, *day));
    for ((campaign, day), v) in camps {
        cn.push(campaigns[campaign].clone());
        ch.push(channels[campaign].clone());
        cd.push(day);
        cs.push(v.sessions);
        cp.push(v.purchasing);
        cr.push(v.revenue);
        ca.push(v.assists)
    }
    output(&out.join("campaign_daily"),"message schema { REQUIRED BINARY campaign_id (UTF8); REQUIRED BINARY channel (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }",vec![Col::Str(cn),Col::Str(ch),Col::Date(cd),Col::I64(cs),Col::I64(cp),Col::I64(cr),Col::I64(ca)])?;
    if metrics {
        eprintln!("output: {:.3}s", mark.elapsed().as_secs_f64());
    }
    Ok(())
}
fn main() {
    if let Some(n) = env::var("NCPU")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
    {
        rayon::ThreadPoolBuilder::new()
            .num_threads(n)
            .build_global()
            .expect("thread pool");
    }
    if let Err(e) = run() {
        eprintln!("shopping-sessions: {e}");
        std::process::exit(1)
    }
}

#[cfg(test)]
mod tests {
    use super::{ClickId, EventKey};
    use std::collections::HashSet;

    #[test]
    fn event_key_is_exact_for_compact_and_fallback_ids() {
        let ids = [b"e1".as_slice(), b"e01", b"E1", b"ezzzzzzzzzzz", b"event-1"];
        let keys: HashSet<_> = ids.iter().map(|id| EventKey::from_bytes(id)).collect();
        assert_eq!(keys.len(), ids.len());
        assert!(matches!(
            EventKey::from_bytes(b"event-1"),
            EventKey::Bytes(_)
        ));
    }

    #[test]
    fn packed_click_ids_keep_bytewise_order() {
        let mut ids = ["k1", "k01", "k10", "k9", "ka", "kz", "k000000000", "K1"];
        let mut keys: Vec<_> = ids
            .iter()
            .map(|id| ClickId::from_bytes(id.as_bytes()))
            .collect();
        ids.sort();
        keys.sort();
        let expected: Vec<_> = ids
            .iter()
            .map(|id| ClickId::from_bytes(id.as_bytes()))
            .collect();
        assert_eq!(keys, expected);
    }
}
