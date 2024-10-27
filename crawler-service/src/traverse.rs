use std::collections::HashSet;

use ego_tree::{iter::Edge, NodeRef};
use scraper::Node;

lazy_static::lazy_static! {
    static ref IGNORED_TAGS: HashSet<&'static str> = {
        let mut s = HashSet::new();
        s.insert("style");
        s.insert("svg");
        s.insert("meta");
        s.insert("canvas");
        s.insert("script");
        s.insert("noscript");
        s.insert("slot");
        s.insert("template");
        s.insert("head");
        s.insert("title");
        s.insert("link");
        s.insert("base");
        s.insert("footer");
        s.insert("header");
        s.insert("nav");
        s.insert("search");
        s.insert("img");
        s.insert("area");
        s.insert("audio");
        s.insert("map");
        s.insert("video");
        s.insert("embed");
        s.insert("iframe");
        s.insert("fencedframe");
        s.insert("object");
        s.insert("picture");
        s.insert("portal");
        s.insert("source");
        s.insert("math");
        s
    };
}

/// Iterator which traverses a subtree.
#[derive(Debug)]
pub struct HtmlTraverse<'a> {
    root: NodeRef<'a, Node>,
    edge: Option<Edge<'a, Node>>,
}

impl<'a> HtmlTraverse<'a> {
    pub fn new(root: NodeRef<'a, Node>) -> Self {
        Self { root, edge: None }
    }
}

impl<'a> Clone for HtmlTraverse<'a> {
    fn clone(&self) -> Self {
        Self {
            root: self.root,
            edge: self.edge,
        }
    }
}

impl<'a> Iterator for HtmlTraverse<'a> {
    type Item = Edge<'a, Node>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.edge {
            None => {
                self.edge = Some(Edge::Open(self.root));
            }
            Some(Edge::Open(node)) => {
                if let Some(element) = node.value().as_element() {
                    if IGNORED_TAGS.contains(element.name()) {
                        self.edge = Some(Edge::Close(node));
                        return self.edge;
                    }
                }
                if let Some(first_child) = node.first_child() {
                    self.edge = Some(Edge::Open(first_child));
                } else {
                    self.edge = Some(Edge::Close(node));
                }
            }
            Some(Edge::Close(mut node)) => {
                if node == self.root {
                    self.edge = None;
                } else {
                    while let Some(next_sibling) = node.next_sibling() {
                        if let Some(element) = next_sibling.value().as_element() {
                            if IGNORED_TAGS.contains(element.name()) {
                                node = next_sibling;
                                continue;
                            }
                        }
                        self.edge = Some(Edge::Open(next_sibling));
                        return self.edge;
                    }
                    self.edge = node.parent().map(Edge::Close);
                }
            }
        }
        self.edge
    }
}
