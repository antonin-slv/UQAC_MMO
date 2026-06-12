use rustc_hash::{FxHashMap, FxHashSet};

pub type FastMap<K, V> = FxHashMap<K, V>;
pub type FastSet<K> = FxHashSet<K>;

pub type Tick = u32; //frames server ???
