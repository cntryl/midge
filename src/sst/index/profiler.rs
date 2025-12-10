//! Key structure profiler for auto-tuning SST index strategy
//!
//! Analyzes key patterns during SST writing to automatically choose between:
//! - SparseIndex (for random/hash-like keys)
//! - TrieIndex (for hierarchical/structured keys)

use std::collections::HashMap;

/// Profile of key structure characteristics
#[derive(Debug, Clone)]
pub struct KeyStructureProfile {
    /// Average bytes of shared prefix between adjacent keys
    pub avg_shared_prefix: f32,
    
    /// Longest prefix shared across any two keys
    pub max_shared_prefix: usize,
    
    /// Number of unique branching points in key space
    pub prefix_divergence: usize,
    
    /// Shannon entropy of key deltas (bits per byte)
    pub entropy: f32,
    
    /// Prefix shared by ALL keys
    pub common_prefix_len: usize,
    
    /// Variance in key lengths
    pub key_length_variance: f32,
    
    /// Sample of hot prefixes (prefix → count)
    pub prefix_heat: Vec<(Vec<u8>, usize)>,
    
    /// Total number of keys profiled
    pub key_count: usize,
}

/// Profiler that analyzes keys during SST construction
pub struct KeyStructureProfiler {
    /// Keys seen so far (we need to keep them for full analysis)
    keys: Vec<Vec<u8>>,
    
    /// Running sum of shared prefix lengths
    shared_prefix_sum: usize,
    
    /// Max shared prefix observed
    max_shared_prefix: usize,
    
    /// Prefix frequency map (first N bytes → count)
    prefix_freq: HashMap<Vec<u8>, usize>,
    
    /// Byte frequency for entropy calculation
    byte_freq: [usize; 256],
    
    /// Total bytes seen
    total_bytes: usize,
    
    /// Prefix length to track for divergence (default 4)
    prefix_track_len: usize,
}

impl KeyStructureProfiler {
    /// Create a new profiler
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            shared_prefix_sum: 0,
            max_shared_prefix: 0,
            prefix_freq: HashMap::new(),
            byte_freq: [0; 256],
            total_bytes: 0,
            prefix_track_len: 4,
        }
    }

    /// Add a key to the profile
    pub fn add_key(&mut self, key: &[u8]) {
        if key.is_empty() {
            return;
        }

        // Calculate shared prefix with previous key
        if let Some(prev_key) = self.keys.last() {
            let shared = lcp(prev_key, key);
            self.shared_prefix_sum += shared;
            self.max_shared_prefix = self.max_shared_prefix.max(shared);
        }

        // Track prefix frequencies (first N bytes)
        let prefix_len = self.prefix_track_len.min(key.len());
        if prefix_len > 0 {
            let prefix = key[..prefix_len].to_vec();
            *self.prefix_freq.entry(prefix).or_insert(0) += 1;
        }

        // Track byte frequencies for entropy
        for &byte in key {
            self.byte_freq[byte as usize] += 1;
            self.total_bytes += 1;
        }

        // Store key
        self.keys.push(key.to_vec());
    }

    /// Finalize and generate profile
    pub fn finish(self) -> KeyStructureProfile {
        let key_count = self.keys.len();

        if key_count == 0 {
            return KeyStructureProfile {
                avg_shared_prefix: 0.0,
                max_shared_prefix: 0,
                prefix_divergence: 0,
                entropy: 0.0,
                common_prefix_len: 0,
                key_length_variance: 0.0,
                prefix_heat: Vec::new(),
                key_count: 0,
            };
        }

        // Calculate average shared prefix
        let avg_shared_prefix = if key_count > 1 {
            self.shared_prefix_sum as f32 / (key_count - 1) as f32
        } else {
            0.0
        };

        // Calculate prefix divergence (number of unique prefixes)
        let prefix_divergence = self.prefix_freq.len();

        // Calculate entropy (Shannon entropy in bits per byte)
        let entropy = self.calculate_entropy();

        // Find common prefix across ALL keys
        let common_prefix_len = self.find_common_prefix_len();

        // Calculate key length variance
        let key_length_variance = self.calculate_key_length_variance();

        // Get top prefix samples
        let mut prefix_heat: Vec<_> = self.prefix_freq.into_iter().collect();
        prefix_heat.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
        prefix_heat.truncate(10); // Keep top 10

        KeyStructureProfile {
            avg_shared_prefix,
            max_shared_prefix: self.max_shared_prefix,
            prefix_divergence,
            entropy,
            common_prefix_len,
            key_length_variance,
            prefix_heat,
            key_count,
        }
    }

    fn calculate_entropy(&self) -> f32 {
        if self.total_bytes == 0 {
            return 0.0;
        }

        let mut entropy = 0.0;
        for &count in &self.byte_freq {
            if count > 0 {
                let p = count as f32 / self.total_bytes as f32;
                entropy -= p * p.log2();
            }
        }

        entropy
    }

    fn find_common_prefix_len(&self) -> usize {
        if self.keys.is_empty() {
            return 0;
        }

        let first_key = &self.keys[0];
        let mut common_len = first_key.len();

        for key in &self.keys[1..] {
            let shared = lcp(first_key, key);
            common_len = common_len.min(shared);
            if common_len == 0 {
                break;
            }
        }

        common_len
    }

    fn calculate_key_length_variance(&self) -> f32 {
        if self.keys.is_empty() {
            return 0.0;
        }

        let mean_len = self.keys.iter().map(|k| k.len()).sum::<usize>() as f32 / self.keys.len() as f32;
        let variance = self.keys.iter()
            .map(|k| {
                let diff = k.len() as f32 - mean_len;
                diff * diff
            })
            .sum::<f32>() / self.keys.len() as f32;

        variance.sqrt()
    }
}

impl Default for KeyStructureProfiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Calculate longest common prefix
fn lcp(a: &[u8], b: &[u8]) -> usize {
    a.iter()
        .zip(b.iter())
        .take_while(|(x, y)| x == y)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_profile_structured_keys() {
        let mut profiler = KeyStructureProfiler::new();
        
        profiler.add_key(b"user/alice/profile");
        profiler.add_key(b"user/alice/settings");
        profiler.add_key(b"user/bob/profile");
        profiler.add_key(b"user/bob/settings");
        
        let profile = profiler.finish();
        
        assert!(profile.avg_shared_prefix >= 5.0); // "user/" shared
        assert!(profile.common_prefix_len >= 4); // "user" shared by all
        assert!(profile.key_count == 4);
    }

    #[test]
    fn should_profile_random_keys() {
        let mut profiler = KeyStructureProfiler::new();
        
        profiler.add_key(b"3c7f4b2a-1234-5678-9abc-def012345678");
        profiler.add_key(b"7f8e9d0c-5678-1234-abcd-ef0123456789");
        profiler.add_key(b"a1b2c3d4-9012-3456-7890-abcdef012345");
        
        let profile = profiler.finish();
        
        assert!(profile.avg_shared_prefix < 2.0); // Random UUIDs share little
        assert!(profile.entropy > 3.0); // High entropy
    }

    #[test]
    fn should_detect_common_prefix() {
        let mut profiler = KeyStructureProfiler::new();
        
        profiler.add_key(b"tenant_123_resource_1");
        profiler.add_key(b"tenant_123_resource_2");
        profiler.add_key(b"tenant_123_resource_3");
        
        let profile = profiler.finish();
        
        assert!(profile.common_prefix_len >= 10); // "tenant_123" shared
    }

    #[test]
    fn should_track_prefix_divergence() {
        let mut profiler = KeyStructureProfiler::new();
        
        // Many different 4-byte prefixes
        for i in 0..100 {
            let key = format!("{:04}_key_{}", i, i);
            profiler.add_key(key.as_bytes());
        }
        
        let profile = profiler.finish();
        
        assert!(profile.prefix_divergence > 50); // Many unique 4-byte prefixes
    }

    #[test]
    fn should_handle_empty_keys() {
        let profiler = KeyStructureProfiler::new();
        let profile = profiler.finish();
        
        assert_eq!(profile.key_count, 0);
        assert_eq!(profile.avg_shared_prefix, 0.0);
    }

    #[test]
    fn should_calculate_key_length_variance() {
        let mut profiler = KeyStructureProfiler::new();
        
        profiler.add_key(b"short");
        profiler.add_key(b"medium_key");
        profiler.add_key(b"very_long_key_with_many_chars");
        
        let profile = profiler.finish();
        
        assert!(profile.key_length_variance > 0.0);
    }
}
