use crate::{parse, Candidate, EventKey};
use hashbrown::{DefaultHashBuilder, HashMap};
use rayon::prelude::*;
use std::{hash::BuildHasher, sync::Mutex};

/// Experimental sharded reduction. Each CSV row belongs to exactly one chunk,
/// even if it spans a chunk boundary.
#[allow(clippy::needless_lifetimes)] // The batch-flush closure names the mapping lifetime.
pub fn reduce<'a>(maps: &'a [Option<memmap2::Mmap>]) -> Vec<HashMap<EventKey<'a>, Candidate>> {
    const SHARDS: usize = 64;
    const CHUNK: usize = 2 * 1024 * 1024;
    const BATCH: usize = 256;
    let shards: Vec<Mutex<Vec<(EventKey<'a>, Candidate)>>> =
        (0..SHARDS).map(|_| Mutex::new(Vec::new())).collect();
    let hash = DefaultHashBuilder::default();
    let mut tasks = Vec::new();
    for (file, map) in maps.iter().enumerate() {
        if let Some(map) = map {
            for start in (0..map.len()).step_by(CHUNK) {
                tasks.push((file, start, (start + CHUNK).min(map.len())));
            }
        }
    }
    tasks.par_iter().for_each(|&(file, start, end)| {
        let map = maps[file].as_ref().unwrap();
        let mut offset = start;
        if start > 0 {
            while offset < map.len() && map[offset - 1] != b'\n' {
                offset += 1;
            }
        } else {
            offset += memchr::memchr(b'\n', &map[offset..]).unwrap_or(map.len() - offset);
            offset = (offset + 1).min(map.len());
        }
        let mut buffers: Vec<Vec<(EventKey<'_>, Candidate)>> =
            (0..SHARDS).map(|_| Vec::new()).collect();
        let flush = |shard: usize, buffer: &mut Vec<(EventKey<'a>, Candidate)>| {
            shards[shard].lock().unwrap().append(buffer);
        };
        while offset < end {
            let begin = offset;
            offset += memchr::memchr(b'\n', &map[offset..]).unwrap_or(map.len() - offset);
            let line = &map[begin..offset];
            offset = (offset + 1).min(map.len());
            let Some(row) = parse::fields::<7>(line) else {
                continue;
            };
            if row[0].is_empty() {
                continue;
            }
            let Some(ingest) = parse::timestamp(row[3]) else {
                continue;
            };
            let id = EventKey::from_bytes(row[0]);
            let shard = (hash.hash_one(id) as usize) % SHARDS;
            buffers[shard].push((
                id,
                Candidate {
                    ingest,
                    file,
                    offset: begin,
                    len: line.len(),
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
        .into_par_iter()
        .map(|shard| {
            let rows = shard.into_inner().unwrap();
            let mut table: HashMap<EventKey<'a>, Candidate> = HashMap::with_capacity(rows.len());
            for (id, candidate) in rows {
                match table.entry(id) {
                    hashbrown::hash_map::Entry::Occupied(mut entry) => {
                        if candidate.ingest < entry.get().ingest {
                            entry.insert(candidate);
                        }
                    }
                    hashbrown::hash_map::Entry::Vacant(entry) => {
                        entry.insert(candidate);
                    }
                }
            }
            table
        })
        .collect()
}
