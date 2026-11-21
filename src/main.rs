mod identity;
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
    collections::{BTreeMap, VecDeque},
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
struct Click {
    person: usize,
    ts: i64,
    campaign: usize,
    id: String,
}
#[derive(Default)]
struct UserDay {
    sessions: i64,
    events: i64,
    revenue: i64,
    categories: HashMap<usize, i64>,
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
    rows::<2>(&data.join("products.csv"), |r| {
        let c = s(r[1]);
        let id = *cat_ids.entry(c.clone()).or_insert_with(|| {
            categories.push(c);
            categories.len() - 1
        });
        products.insert(s(r[0]), id);
    })?;
    let mut campaigns = vec!["organic".to_string()];
    let mut channels = vec!["organic".to_string()];
    let mut camp_ids = HashMap::new();
    rows::<2>(&data.join("campaigns.csv"), |r| {
        let id = campaigns.len();
        campaigns.push(s(r[0]));
        channels.push(s(r[1]));
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
    let mut events = Vec::with_capacity(tables.iter().map(|t| t.len()).sum());
    let candidates = tables.into_iter().flat_map(|table| table.into_values());
    let candidates: Box<dyn Iterator<Item = Candidate>> =
        if env::var_os("SHOPPING_UNORDERED_ROWS").is_none() {
            let mut ordered: Vec<_> = candidates.collect();
            ordered.par_sort_unstable_by_key(|e| (e.file, e.offset));
            Box::new(ordered.into_iter())
        } else {
            Box::new(candidates)
        };
    for e in candidates {
        let map = maps[e.file].as_ref().unwrap();
        let r = parse::fields::<7>(&map[e.offset..e.offset + e.len]).unwrap();
        let Some(ts) = parse::timestamp(r[2]) else {
            continue;
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
            continue;
        }
        events.push(Event {
            person: people.resolve(std::str::from_utf8(r[1])?),
            ts,
            price: price.unwrap(),
            category: *products.get(std::str::from_utf8(r[5])?).unwrap_or(&0),
        });
    }
    drop(maps);
    if end - start <= i64::from(u32::MAX) && people.len() <= u32::MAX as usize {
        events.par_sort_unstable_by_key(|e| ((e.person as u64) << 32) | (e.ts - start) as u64);
    } else {
        events.par_sort_unstable_by_key(|e| (e.person, e.ts));
    }
    stage!("event validation and sorting");
    let mut cmap: HashMap<String, Click> = HashMap::new();
    for p in files(&data.join("clicks"))? {
        rows::<4>(&p, |r| {
            if r[0].is_empty() || r[1].is_empty() {
                return;
            }
            let Some(ts) = parse::timestamp(r[3]) else {
                return;
            };
            if ts < start - WEEK || ts >= end {
                return;
            }
            let Ok(campaign_id) = std::str::from_utf8(r[2]) else {
                return;
            };
            let Some(&campaign) = camp_ids.get(campaign_id) else {
                return;
            };
            let id = s(r[0]);
            let Ok(user) = std::str::from_utf8(r[1]) else {
                return;
            };
            let person = people.resolve(user);
            cmap.entry(id.clone()).or_insert(Click {
                person,
                ts,
                campaign,
                id,
            });
        })?;
    }
    let mut clicks: Vec<Click> = cmap.into_values().collect();
    clicks.par_sort_unstable_by(|a, b| (a.person, a.ts, &a.id).cmp(&(b.person, b.ts, &b.id)));
    stage!("click parsing and sorting");
    let mut users: BTreeMap<(usize, i32), UserDay> = BTreeMap::new();
    let mut camps: BTreeMap<(usize, i32), CampDay> = BTreeMap::new();
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
        let mut active = VecDeque::new();
        let mut counts: HashMap<usize, usize> = HashMap::new();
        while ei < events.len() && events[ei].person == person {
            let st = events[ei].ts;
            let day = st.div_euclid(DAY) as i32;
            let (mut last, mut count, mut revenue) = (st, 0, 0);
            let mut cats = HashMap::new();
            while ei < events.len()
                && events[ei].person == person
                && events[ei].ts - last <= 1_800_000
            {
                let e = &events[ei];
                last = e.ts;
                count += 1;
                revenue += e.price;
                if e.price > 0 {
                    *cats.entry(e.category).or_insert(0) += e.price
                }
                ei += 1
            }
            let u = users.entry((person, day)).or_default();
            u.sessions += 1;
            u.events += count;
            u.revenue += revenue;
            for (k, v) in cats {
                *u.categories.entry(k).or_insert(0) += v
            }
            while add < clicks.len() && clicks[add].ts <= st {
                active.push_back(add);
                *counts.entry(clicks[add].campaign).or_insert(0) += 1;
                add += 1
            }
            while active.front().is_some_and(|&i| clicks[i].ts < st - WEEK) {
                let i = active.pop_front().unwrap();
                let n = counts.get_mut(&clicks[i].campaign).unwrap();
                *n -= 1;
                if *n == 0 {
                    counts.remove(&clicks[i].campaign);
                }
            }
            let credited = if let Some(&i) = active.back() {
                let ts = clicks[i].ts;
                let j = clicks.partition_point(|c| c.ts < ts);
                clicks[j].campaign
            } else {
                0
            };
            let c = camps.entry((credited, day)).or_default();
            c.sessions += 1;
            c.revenue += revenue;
            c.purchasing += i64::from(revenue > 0);
            for &campaign in counts.keys() {
                camps.entry((campaign, day)).or_default().assists += 1
            }
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
                    vb.cmp(va)
                        .then_with(|| categories[**a].cmp(&categories[**b]))
                })
                .map(|(c, _)| categories[*c].clone()),
        );
    }
    output(&out.join("user_daily"),"message schema { REQUIRED BINARY user_id (UTF8); REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; OPTIONAL BINARY top_category (UTF8); }",vec![Col::Str(un),Col::Date(ud),Col::I64(us),Col::I64(ue),Col::I64(ur),Col::Opt(ut)])?;
    let (mut cn, mut ch, mut cd, mut cs, mut cp, mut cr, mut ca) =
        (vec![], vec![], vec![], vec![], vec![], vec![], vec![]);
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
    use super::EventKey;
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
}
