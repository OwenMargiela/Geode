#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::index::tree::{index_types::NodeKey, tree_node::tree_node_inner::NodeInner};

impl NodeInner {
    pub fn split(&mut self, b: usize) -> anyhow::Result<(NodeKey, NodeInner)> {
        unimplemented!()
    }

    pub fn can_borrow(&self) -> bool {
        unimplemented!()
    }

    pub fn borrow(
        &mut self,
        current_parent_node: &mut NodeInner,
        current_candidate: &mut NodeInner,
        is_left: bool,
        separator_key: NodeKey,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn merge(&mut self, sibling: &NodeInner) -> anyhow::Result<()> {
        unimplemented!()
    }
}
