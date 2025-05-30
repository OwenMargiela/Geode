#![allow(unused_variables)] 
#![allow(dead_code)] 

use super::byte_box::ByteBox;

#[derive(PartialEq, Eq, Clone, Debug, PartialOrd, Ord)]
pub struct KeyValuePair {
    pub(crate) key: ByteBox,
    pub(crate) value: ByteBox,
}

#[derive(PartialEq, Eq, Clone, Debug, PartialOrd, Ord)]
pub enum NodeKey {
    GuidePost(ByteBox),
    KeyValuePair(KeyValuePair),
}

impl NodeKey {
    pub fn to_kv_pair(&self) -> anyhow::Result<KeyValuePair> {
        match self {
            NodeKey::KeyValuePair(kv) => Ok(kv.clone()),

            _ => {
                return Err(anyhow::Error::msg("Unable to convert to kv pair"));
            }
        }
    }

    pub fn to_guide_post(&self) -> anyhow::Result<ByteBox> {
        match self {
            NodeKey::GuidePost(k) => Ok(k.clone()),

            _ => {
                return Err(anyhow::Error::msg("Unable to convert to guide post key"));
            }
        }
    }
}
