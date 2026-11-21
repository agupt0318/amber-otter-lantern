use crate::{campaign_number, parse, ClickId, PendingClick};
use hashbrown::{DefaultHashBuilder, HashMap};
use rayon::prelude::*;
use std::{hash::BuildHasher, sync::Mutex};

struct Candidate<'a> {
    file: usize,
    offset: usize,
    click: PendingClick<'a>,
}

/// Keeps the first valid click in sorted-file, byte-offset order while parsing
/// independent file chunks in parallel.
pub fn reduce<'a>(
    maps: &'a [Option<memmap2::Mmap>],
    start: i64,
    end: i64,
    campaigns: &HashMap<String, usize>,
    campaign_ids: &[usize],
) -> Vec<HashMap<ClickId, PendingClick<'a>>> {
    const SHARDS: usize = 64;
    const CHUNK: usize = 2 * 1024 * 1024;
    const BATCH: usize = 256;
    let shards: Vec<Mutex<HashMap<ClickId, Candidate<'a>>>> =
        (0..SHARDS).map(|_| Mutex::new(HashMap::new())).collect();
    let hash = DefaultHashBuilder::default();
    let mut tasks = vec![];
    for (file, map) in maps.iter().enumerate() {
        if let Some(map) = map {
            for offset in (0..map.len()).step_by(CHUNK) {
                tasks.push((file, offset, (offset + CHUNK).min(map.len())));
            }
        }
    }
    tasks
        .par_iter()
        .for_each(|&(file, start_offset, end_offset)| {
            let map = maps[file].as_ref().unwrap();
            let mut offset = start_offset;
            if start_offset > 0 {
                while offset < map.len() && map[offset - 1] != b'\n' {
                    offset += 1;
                }
            } else {
                offset += memchr::memchr(b'\n', &map[offset..]).unwrap_or(map.len() - offset);
                offset = (offset + 1).min(map.len());
            }
            let mut buffers: Vec<Vec<(ClickId, Candidate<'a>)>> =
                (0..SHARDS).map(|_| Vec::new()).collect();
            let flush = |shard: usize, buffer: &mut Vec<(ClickId, Candidate<'a>)>| {
                let mut table = shards[shard].lock().unwrap();
                for (id, candidate) in buffer.drain(..) {
                    if table.get(&id).is_some_and(|old| {
                        (old.file, old.offset) <= (candidate.file, candidate.offset)
                    }) {
                        continue;
                    }
                    table.insert(id, candidate);
                }
            };
            while offset < end_offset {
                let begin = offset;
                offset += memchr::memchr(b'\n', &map[offset..]).unwrap_or(map.len() - offset);
                let line = &map[begin..offset];
                offset = (offset + 1).min(map.len());
                let Some(row) = parse::fields::<4>(line) else {
                    continue;
                };
                if row[0].is_empty() || row[1].is_empty() {
                    continue;
                }
                let Some(ts) = parse::timestamp(row[3]) else {
                    continue;
                };
                if ts < start || ts >= end {
                    continue;
                }
                let campaign =
                    match campaign_number(row[2]).and_then(|number| campaign_ids.get(number)) {
                        Some(&campaign) if campaign != 0 => campaign,
                        _ => {
                            let Ok(campaign_id) = std::str::from_utf8(row[2]) else {
                                continue;
                            };
                            let Some(&campaign) = campaigns.get(campaign_id) else {
                                continue;
                            };
                            campaign
                        }
                    };
                let Ok(user) = std::str::from_utf8(row[1]) else {
                    continue;
                };
                let id = ClickId::from_bytes(row[0]);
                let shard = (hash.hash_one(&id) as usize) % SHARDS;
                buffers[shard].push((
                    id,
                    Candidate {
                        file,
                        offset: begin,
                        click: PendingClick { user, ts, campaign },
                    },
                ));
                if buffers[shard].len() >= BATCH {
                    flush(shard, &mut buffers[shard]);
                }
            }
            for (shard, buffer) in buffers.iter_mut().enumerate() {
                if !buffer.is_empty() {
                    flush(shard, buffer);
                }
            }
        });
    shards
        .into_iter()
        .map(|shard| {
            shard
                .into_inner()
                .unwrap()
                .into_iter()
                .map(|(id, candidate)| (id, candidate.click))
                .collect()
        })
        .collect()
}
