#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::byte_box::ByteBox;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct KeyValuePair {
    pub(crate) key: ByteBox,
    pub(crate) value: ByteBox,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum NodeKey {
    GuidePost(ByteBox),
    KeyValuePair(KeyValuePair),
}

impl NodeKey {
    pub fn to_kv_pair(&self) -> anyhow::Result<KeyValuePair> {
        match self {
            NodeKey::KeyValuePair(kv) => Ok(kv.clone()),

            _ => return Err(anyhow::Error::msg("Unable to convert to kv pair")),
        }
    }

    pub fn to_guide_post(&self) -> anyhow::Result<ByteBox> {
        match self {
            NodeKey::GuidePost(k) => Ok(k.clone()),

            _ => return Err(anyhow::Error::msg("Unable to convert to guide post key")),
        }
    }
}
