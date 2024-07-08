use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use derive_builder::Builder;
use num_enum::TryFromPrimitive;

use crate::api::SourcetrailError;

#[derive(Debug)]
pub struct Meta {
    id: i64,
    key: String,
    value: String,
}

impl Meta {
    pub fn new(id: i64, key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            id,
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

#[derive(Debug)]
pub struct Element {
    id: i64,
}

impl AsRef<Element> for Element {
    fn as_ref(&self) -> &Element {
        self
    }
}

impl Element {
    pub fn new(id: i64) -> Self {
        Element { id }
    }

    pub fn id(&self) -> i64 {
        self.id
    }
}

#[derive(Debug, Copy, Clone, TryFromPrimitive)]
#[repr(i32)]
pub enum ElementComponentType {
    None = 0,
    IsAmbiguous = 1,
}

#[derive(Debug)]
pub struct ElementComponent {
    id: i64,
    elem_id: i64,
    type_: ElementComponentType,
    data: String,
}

impl AsRef<ElementComponent> for ElementComponent {
    fn as_ref(&self) -> &ElementComponent {
        self
    }
}

impl ElementComponent {
    pub fn new(
        id: i64,
        elem_id: i64,
        type_: ElementComponentType,
        data: impl Into<String>,
    ) -> Self {
        ElementComponent {
            id,
            elem_id,
            type_,
            data: data.into(),
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn elem_id(&self) -> i64 {
        self.elem_id
    }

    pub fn component_type(&self) -> ElementComponentType {
        self.type_
    }

    pub fn data(&self) -> &str {
        &self.data
    }
}

#[derive(Debug, Copy, Clone, TryFromPrimitive)]
#[repr(i32)]
pub enum EdgeType {
    Undefined = 0,
    Member = 1 << 0,
    TypeUsage = 1 << 1,
    Usage = 1 << 2,
    Call = 1 << 3,
    Inheritance = 1 << 4,
    Override = 1 << 5,
    TypeArgument = 1 << 6,
    TemplateSpecialization = 1 << 7,
    Include = 1 << 8,
    Import = 1 << 9,
    BundledEdges = 1 << 10,
    MacroUsage = 1 << 11,
    AnnotationUsage = 1 << 12,
}

#[derive(Debug)]
pub struct Edge {
    id: i64,
    type_: EdgeType,
    src: i64,
    dst: i64,
}

impl AsRef<Edge> for Edge {
    fn as_ref(&self) -> &Edge {
        self
    }
}

impl Edge {
    pub fn new(id: i64, type_: EdgeType, src: i64, dst: i64) -> Self {
        Edge {
            id,
            type_,
            src,
            dst,
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn type_(&self) -> EdgeType {
        self.type_
    }

    pub fn source_id(&self) -> i64 {
        self.src
    }

    pub fn target_id(&self) -> i64 {
        self.dst
    }
}

#[derive(Debug, Copy, Clone, TryFromPrimitive)]
#[repr(i32)]
pub enum NodeType {
    NodeSymbol = 1 << 0,
    NodeType = 1 << 1,
    NodeBuiltinType = 1 << 2,
    NodeModule = 1 << 3,
    NodeNamespace = 1 << 4,
    NodePackage = 1 << 5,
    NodeStruct = 1 << 6,
    NodeClass = 1 << 7,
    NodeInterface = 1 << 8,
    NodeAnnotation = 1 << 9,
    NodeGlobalVariable = 1 << 10,
    NodeField = 1 << 11,
    NodeFunction = 1 << 12,
    NodeMethod = 1 << 13,
    NodeEnum = 1 << 14,
    NodeEnumConstant = 1 << 15,
    NodeTypedef = 1 << 16,
    NodeTypeParameter = 1 << 17,
    NodeFile = 1 << 18,
    NodeMacro = 1 << 19,
    NodeUnion = 1 << 20,
}

#[derive(Debug)]
pub struct Node {
    id: i64,
    type_: NodeType,
    name: String,
}

impl AsRef<Node> for Node {
    fn as_ref(&self) -> &Node {
        self
    }
}

impl Node {
    pub fn new(id: i64, type_: NodeType, name: impl Into<String>) -> Self {
        Node {
            id,
            type_,
            name: name.into(),
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn type_(&self) -> NodeType {
        self.type_
    }

    pub fn set_type(&mut self, type_: NodeType) {
        self.type_ = type_;
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(i32)]
pub enum SymbolType {
    None = 0,
    Implicit = 1,
    Explicit = 2,
}

#[derive(Debug)]
pub struct Symbol {
    id: i64,
    definition_kind: SymbolType,
}

impl AsRef<Symbol> for Symbol {
    fn as_ref(&self) -> &Symbol {
        self
    }
}

impl Symbol {
    pub fn new(id: i64, definition_kind: SymbolType) -> Self {
        Symbol {
            id,
            definition_kind,
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn definition_kind(&self) -> SymbolType {
        self.definition_kind
    }

    pub fn set_definition_kind(&mut self, kind: SymbolType) {
        self.definition_kind = kind;
    }
}

#[derive(Debug, Builder)]
#[builder(setter(into))]
pub struct File {
    id: i64,
    path: PathBuf,
    language: String,
    modification_time: DateTime<Utc>,
    indexed: bool,
    complete: bool,
    line_count: u32,
}

impl FileBuilder {
    pub fn new() -> Self {
        FileBuilder::default()
    }
}

impl AsRef<File> for File {
    fn as_ref(&self) -> &File {
        self
    }
}

impl File {
    pub fn new(
        id: i64,
        path: impl AsRef<Path>,
        language: impl Into<String>,
        modification_time: DateTime<Utc>,
        indexed: bool,
        complete: bool,
        line_count: u32,
    ) -> Self {
        File {
            id,
            path: path.as_ref().to_path_buf(),
            language: language.into(),
            modification_time,
            indexed,
            complete,
            line_count,
        }
    }

    pub fn builder() -> FileBuilder {
        FileBuilder::new()
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn modification_time(&self) -> DateTime<Utc> {
        self.modification_time
    }

    pub(crate) fn modification_time_str(&self) -> String {
        self.modification_time()
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    }

    pub fn language(&self) -> &str {
        &self.language
    }

    pub fn set_language(&mut self, language: impl Into<String>) {
        self.language = language.into();
    }

    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    pub fn is_complete(&self) -> bool {
        self.complete
    }

    pub fn line_count(&self) -> u32 {
        self.line_count
    }
}

#[derive(Debug)]
pub struct FileContent {
    id: i64,
    content: String,
}

impl AsRef<FileContent> for FileContent {
    fn as_ref(&self) -> &FileContent {
        self
    }
}

impl FileContent {
    pub fn new(id: i64, content: impl Into<String>) -> Self {
        FileContent {
            id,
            content: content.into(),
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn set_content(&mut self, content: impl Into<String>) {
        self.content = content.into();
    }
}

#[derive(Debug)]
pub struct LocalSymbol {
    id: i64,
    name: String,
}

impl AsRef<LocalSymbol> for LocalSymbol {
    fn as_ref(&self) -> &LocalSymbol {
        self
    }
}

impl LocalSymbol {
    pub fn new(id: i64, name: impl Into<String>) -> Self {
        LocalSymbol {
            id,
            name: name.into(),
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = name.into();
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(i32)]
pub enum SourceLocationType {
    Token = 0,
    Scope = 1,
    Qualifier = 2,
    LocalSymbol = 3,
    Signature = 4,
    AtomicRange = 5,
    IndexerError = 6,
    FulltextSearch = 7,
    ScreenSearch = 8,
    Unsolved = 9,
}

#[derive(Debug, Builder)]
#[builder(setter(into), build_fn(validate = "Self::validate"))]
pub struct SourceLocation {
    id: i64,
    file_node_id: i64,
    start_line: i32,
    start_column: i32,
    end_line: i32,
    end_column: i32,
    #[builder(setter(name = "location_type"))]
    type_: SourceLocationType,
}

impl SourceLocationBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    fn validate(&self) -> Result<(), String> {
        if let Some((start, end)) = self.start_line.and_then(|s| self.end_line.map(|e| (s, e))) {
            if start > end {
                return Err(String::from("start_line must be less than end_line"));
            }

            if start == end {
                if let Some((start, end)) = self
                    .start_column
                    .and_then(|s| self.end_column.map(|e| (s, e)))
                {
                    if start >= end {
                        return Err(String::from(
                            "start_column must be less than end_column (same line)",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

impl AsRef<SourceLocation> for SourceLocation {
    fn as_ref(&self) -> &SourceLocation {
        self
    }
}

impl SourceLocation {
    pub fn new(
        id: i64,
        file_node_id: i64,
        start_line: i32,
        start_column: i32,
        end_line: i32,
        end_column: i32,
        type_: SourceLocationType,
    ) -> Result<Self, SourcetrailError> {
        if start_line > end_line {
            return Err(SourcetrailError::InvalidSourceRange);
        }

        if start_line == end_line && start_column >= end_column {
            return Err(SourcetrailError::InvalidSourceRange);
        }

        Ok(SourceLocation {
            id,
            file_node_id,
            start_line,
            start_column,
            end_line,
            end_column,
            type_,
        })
    }

    pub fn builder(self) -> SourceLocationBuilder {
        SourceLocationBuilder::default()
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn file_node_id(&self) -> i64 {
        self.file_node_id
    }

    pub fn start_line(&self) -> i32 {
        self.start_line
    }

    pub fn start_column(&self) -> i32 {
        self.start_column
    }

    pub fn end_line(&self) -> i32 {
        self.end_line
    }

    pub fn end_column(&self) -> i32 {
        self.end_column
    }

    pub fn location_type(&self) -> SourceLocationType {
        self.type_
    }
}

#[derive(Debug)]
pub struct Occurrence {
    element_id: i64,
    source_location_id: i64,
}

impl AsRef<Occurrence> for Occurrence {
    fn as_ref(&self) -> &Occurrence {
        self
    }
}

impl Occurrence {
    pub fn new(element_id: i64, source_location_id: i64) -> Self {
        Occurrence {
            element_id,
            source_location_id,
        }
    }

    pub fn element_id(&self) -> i64 {
        self.element_id
    }

    pub fn source_location_id(&self) -> i64 {
        self.source_location_id
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, TryFromPrimitive)]
#[repr(i32)]
pub enum ComponentAccessType {
    None = 0,
    Public = 1,
    Protected = 2,
    Private = 3,
    Default = 4,
    TemplateParameter = 5,
    TypeParameter = 6,
}

#[derive(Debug)]
pub struct ComponentAccess {
    node_id: i64,
    type_: ComponentAccessType,
}

impl ComponentAccess {
    pub fn new(node_id: i64, type_: ComponentAccessType) -> Self {
        ComponentAccess { node_id, type_ }
    }

    pub fn id(&self) -> i64 {
        self.node_id
    }

    pub fn access_type(&self) -> ComponentAccessType {
        self.type_
    }
}

#[derive(Debug, Default, Builder)]
pub struct Error {
    id: i64,
    #[builder(setter(into))]
    message: String,
    #[builder(default)]
    fatal: bool,
    #[builder(default)]
    indexed: bool,
    #[builder(setter(into))]
    translation_unit: String,
}

impl ErrorBuilder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AsRef<Error> for Error {
    fn as_ref(&self) -> &Error {
        self
    }
}

impl Error {
    pub fn new(
        id: i64,
        message: impl Into<String>,
        fatal: bool,
        indexed: bool,
        translation_unit: impl Into<String>,
    ) -> Self {
        Error {
            id,
            message: message.into(),
            fatal,
            indexed,
            translation_unit: translation_unit.into(),
        }
    }

    pub fn id(&self) -> i64 {
        self.id
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_fatal(&self) -> bool {
        self.fatal
    }

    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    pub fn translation_unit(&self) -> &str {
        &self.translation_unit
    }
}

#[derive(Debug)]
pub struct NameHierarchy {
    delimiter: String,
    elements: Vec<NameElement>,
}

impl NameHierarchy {
    pub const DELIMITER: &'static str = "\t";
    pub const META_DELIMITER: &'static str = "\tm";
    pub const NAME_DELIMITER: &'static str = "\tn";
    pub const PART_DELIMITER: &'static str = "\ts";
    pub const SIGNATURE_DELIMITER: &'static str = "\tp";

    pub const NAME_DELIMITER_FILE: &'static str = "/";
    pub const NAME_DELIMITER_CXX: &'static str = "::";
    pub const NAME_DELIMITER_JAVA: &'static str = ".";
    pub const NAME_DELIMITER_UNKNOWN: &'static str = "@";

    pub fn new(
        delimiter: impl Into<String>,
        elements: impl IntoIterator<Item = NameElement>,
    ) -> Result<Self, SourcetrailError> {
        let elements = elements.into_iter().collect::<Vec<_>>();

        if elements.is_empty() {
            return Err(SourcetrailError::EmptyNameHierarchy);
        }

        Ok(NameHierarchy {
            delimiter: delimiter.into(),
            elements,
        })
    }

    pub fn delimiter(&self) -> &str {
        &self.delimiter
    }

    pub fn names(&self) -> &[NameElement] {
        &self.elements
    }

    pub fn size(&self) -> usize {
        self.elements.len()
    }

    pub fn push_element(&mut self, element: NameElement) {
        self.elements.push(element);
    }

    pub fn extend_elements(&mut self, elements: impl IntoIterator<Item = NameElement>) {
        self.elements.extend(elements);
    }

    pub fn serialize_range(&self, start: usize, end: usize) -> Result<String, SourcetrailError> {
        if start >= end || end > self.elements.len() {
            return Err(SourcetrailError::Serialize);
        }
        let serialized = self.elements[start..end]
            .iter()
            .map(|e| {
                format!(
                    "{}\ts{}\tp{}",
                    e.name.as_deref().unwrap_or_default(),
                    e.prefix.as_deref().unwrap_or_default(),
                    e.postfix.as_deref().unwrap_or_default()
                )
            })
            .collect::<Vec<String>>()
            .join("\tn");

        Ok(format!("{}\tm{}", self.delimiter, serialized))
    }

    pub fn serialize_name(&self) -> Result<String, SourcetrailError> {
        self.serialize_range(0, self.size())
    }

    pub fn deserialize_name(
        serialized_name: impl AsRef<str>,
    ) -> Result<NameHierarchy, SourcetrailError> {
        let serialized_name = serialized_name.as_ref();
        let idx = serialized_name
            .find("\tm")
            .ok_or(SourcetrailError::Deserialize)?;

        let delimiter = &serialized_name[0..idx];
        let mut elements = Vec::new();

        let parts = serialized_name[idx + 2..].split("\tn").collect::<Vec<_>>();

        for part in parts {
            let segments = part.split("\ts").collect::<Vec<_>>();
            if segments.len() < 2 {
                return Err(SourcetrailError::Deserialize);
            }

            let name = segments[0].to_owned();
            let signature_postfix = segments[1].split("\tp").collect::<Vec<_>>();
            if signature_postfix.len() < 2 {
                return Err(SourcetrailError::Deserialize);
            }

            let prefix = signature_postfix[0].to_owned();
            let postfix = signature_postfix[1].to_owned();
            elements.push(
                NameElementBuilder::new()
                    .prefix(prefix)
                    .name(name)
                    .postfix(postfix)
                    .build(),
            );
        }

        NameHierarchy::new(delimiter, elements)
    }
}

#[derive(Debug, Builder, Default)]
#[builder(build_fn(skip), pattern = "owned")]
pub struct NameElement {
    #[builder(setter(into, strip_option))]
    prefix: Option<String>,
    #[builder(setter(into, strip_option))]
    name: Option<String>,
    #[builder(setter(into, strip_option))]
    postfix: Option<String>,
}

impl NameElementBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> NameElement {
        NameElement::new(
            self.prefix.flatten(),
            self.name.flatten(),
            self.postfix.flatten(),
        )
    }
}

impl AsRef<NameElement> for NameElement {
    fn as_ref(&self) -> &NameElement {
        self
    }
}

impl NameElement {
    pub fn new(
        prefix: impl Into<Option<String>>,
        name: impl Into<Option<String>>,
        postfix: impl Into<Option<String>>,
    ) -> Self {
        NameElement {
            prefix: prefix.into(),
            name: name.into(),
            postfix: postfix.into(),
        }
    }

    pub fn builder() -> NameElementBuilder {
        NameElementBuilder::new()
    }

    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn postfix(&self) -> Option<&str> {
        self.postfix.as_deref()
    }

    pub fn set_prefix(&mut self, prefix: impl Into<String>) {
        self.prefix = Some(prefix.into());
    }

    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = Some(name.into());
    }

    pub fn set_postfix(&mut self, postfix: impl Into<String>) {
        self.postfix = Some(postfix.into());
    }
}
