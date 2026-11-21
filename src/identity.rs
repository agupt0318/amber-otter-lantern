use hashbrown::HashMap;

/// Exact, case-sensitive ID interning with component minima independent of root.
#[derive(Default)]
pub struct People {
    packed_ids: HashMap<u64, usize>,
    other_ids: HashMap<String, usize>,
    names: Vec<String>,
    parent: Vec<usize>,
    size: Vec<usize>,
    minimum: Vec<usize>,
}

impl People {
    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn intern(&mut self, id: &str) -> usize {
        if let Some(key) = packed_id(id.as_bytes()) {
            if let Some(&index) = self.packed_ids.get(&key) {
                return index;
            }
            let index = self.names.len();
            self.packed_ids.insert(key, index);
            self.names.push(id.to_owned());
            self.parent.push(index);
            self.size.push(1);
            self.minimum.push(index);
            return index;
        }
        if let Some(&index) = self.other_ids.get(id) {
            return index;
        }
        let index = self.names.len();
        self.other_ids.insert(id.to_owned(), index);
        self.names.push(id.to_owned());
        self.parent.push(index);
        self.size.push(1);
        self.minimum.push(index);
        index
    }

    pub fn root(&mut self, mut index: usize) -> usize {
        while self.parent[index] != index {
            self.parent[index] = self.parent[self.parent[index]];
            index = self.parent[index];
        }
        index
    }

    pub fn link(&mut self, a: &str, b: &str) {
        let a = self.intern(a);
        let b = self.intern(b);
        let mut a = self.root(a);
        let mut b = self.root(b);
        if a == b {
            return;
        }
        if self.size[a] < self.size[b] {
            std::mem::swap(&mut a, &mut b);
        }
        self.parent[b] = a;
        self.size[a] += self.size[b];
        if self.names[self.minimum[b]] < self.names[self.minimum[a]] {
            self.minimum[a] = self.minimum[b];
        }
    }

    /// Resolves an ID after the caller has interned every ID it will query.
    /// This read-only form is safe to use from parallel event materialization.
    pub fn resolve_known(&self, id: &str) -> Option<usize> {
        let index = if let Some(key) = packed_id(id.as_bytes()) {
            *self.packed_ids.get(&key)?
        } else {
            *self.other_ids.get(id)?
        };
        Some(self.root_readonly(index))
    }

    fn root_readonly(&self, mut index: usize) -> usize {
        while self.parent[index] != index {
            index = self.parent[index];
        }
        index
    }

    pub fn canonical(&mut self, id: usize) -> &str {
        let root = self.root(id);
        &self.names[self.minimum[root]]
    }
}

/// Losslessly packs the common generator IDs (one prefix byte plus up to nine
/// lowercase base-36 bytes). Anything else stays in the exact string table.
#[inline]
pub(crate) fn packed_id(id: &[u8]) -> Option<u64> {
    if id.len() < 2 || id.len() > 10 {
        return None;
    }
    let mut value = 1u64;
    for &byte in &id[1..] {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'z' => byte - b'a' + 10,
            _ => return None,
        };
        value = value * 37 + u64::from(digit) + 1;
    }
    Some((value << 8) | u64::from(id[0]))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_transitive_components_and_link_only_minimum() {
        let mut p = People::default();
        p.link("z", "b");
        p.link("b", "A");
        p.link("z", "b");
        let z = p.intern("z");
        assert_eq!(p.canonical(z), "A");
        let a = p.intern("a");
        assert_ne!(z, a);
        assert_eq!(p.canonical(a), "a");
    }

    #[test]
    fn compact_user_ids_do_not_merge_distinct_spellings() {
        let mut p = People::default();
        let a = p.intern("u1");
        let b = p.intern("u01");
        let c = p.intern("U1");
        let d = p.intern("u-1");
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, d);
    }
}
