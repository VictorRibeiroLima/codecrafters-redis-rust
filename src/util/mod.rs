use std::hash::{Hash, Hasher};

pub fn gen_rand_string(len: usize) -> String {
    let mut s = String::new();
    for _ in 0..len {
        let c = std::char::from_u32(gen_rand_number() % 26 + 97).unwrap();
        s.push(c);
    }
    s
}

pub fn gen_rand_number() -> u32 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    millis.hash(&mut hasher);
    hasher.finish() as u32
}

#[cfg(test)]
mod test {
    use crate::util::gen_rand_string;

    #[test]
    fn test_gen_rand_string() {
        let s1 = gen_rand_string(40);
        let s2 = gen_rand_string(40);
        assert_ne!(s1, s2);
    }
}
