use hashbrown::HashMap;

/// Exact, case-sensitive ID interning with component minima independent of root.
#[derive(Default)]
pub struct People {
    ids: HashMap<String, usize>,
    names: Vec<String>,
    parent: Vec<usize>,
    size: Vec<usize>,
    minimum: Vec<usize>,
}

impl People {
    pub fn intern(&mut self, id: &str) -> usize {
        if let Some(&index) = self.ids.get(id) {
            return index;
        }
        let index = self.names.len();
        self.ids.insert(id.to_owned(), index);
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

    pub fn resolve(&mut self, id: &str) -> usize {
        let id = self.intern(id);
        self.root(id)
    }

    pub fn canonical(&mut self, id: usize) -> &str {
        let root = self.root(id);
        &self.names[self.minimum[root]]
    }
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
        let z = p.resolve("z");
        assert_eq!(p.canonical(z), "A");
        let a = p.resolve("a");
        assert_ne!(z, a);
        assert_eq!(p.canonical(a), "a");
    }
}
