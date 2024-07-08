use std::borrow::Cow;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use async_std::io::ReadExt;
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use thiserror::Error;

use crate::db::*;
use crate::types::*;

#[derive(Debug, Error)]
pub enum SourcetrailError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("deserialization error")]
    Deserialize,
    #[error("serialization error")]
    Serialize,
    #[error("file error: {0}")]
    File(String),
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("database not open")]
    NoDatabaseOpen,
    #[error("parent with id {0} does not exist in the database")]
    ParentNotFound(i64),
    #[error("file with id {0} does not exist in the database")]
    FileNotFound(i64),
    #[error("name hierarchy must contain at least one element")]
    EmptyNameHierarchy,
    #[error("invalid source range")]
    InvalidSourceRange,
    #[error("cannot build type: {0}")]
    Builder(anyhow::Error),
    #[error("cannot commit error location record: {0}")]
    ErrorLocationBuilder(anyhow::Error),
    #[error("cannot commit file record: {0}")]
    FileRecorder(anyhow::Error),
    #[error("cannot commit source location record: {0}")]
    SourceLocationBuilder(anyhow::Error),
    #[error("cannot commit unsolved symbol record: {0}")]
    UnsolvedSymbolBuilder(anyhow::Error),
    #[error("cannot convert type to/from database represenation: {0}")]
    TypeConversion(anyhow::Error),
}

impl SourcetrailError {
    pub fn convert<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::TypeConversion(anyhow::Error::from(e))
    }

    pub fn builder<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::Builder(anyhow::Error::from(e))
    }

    pub fn error_location<M>(m: M) -> Self
    where
        M: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static,
    {
        Self::ErrorLocationBuilder(anyhow::Error::msg(m))
    }

    pub fn file_recorder<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::FileRecorder(anyhow::Error::from(e))
    }

    pub fn file_recorder_with<M>(m: M) -> Self
    where
        M: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static,
    {
        Self::FileRecorder(anyhow::Error::msg(m))
    }

    pub fn source_location<M>(m: M) -> Self
    where
        M: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static,
    {
        Self::SourceLocationBuilder(anyhow::Error::msg(m))
    }

    pub fn unsolved_symbol<M>(m: M) -> Self
    where
        M: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static,
    {
        Self::UnsolvedSymbolBuilder(anyhow::Error::msg(m))
    }
}

pub struct NodeRecorder<'a, 'b> {
    db: &'a mut SourcetrailDB,
    name: Cow<'b, str>,
    prefix: Cow<'b, str>,
    postfix: Cow<'b, str>,
    delimiter: Cow<'b, str>,
    parent_id: Option<i64>,
    is_indexed: bool,
    node_type: NodeType,
}

impl<'a, 'b> NodeRecorder<'a, 'b> {
    pub fn new(db: &'a mut SourcetrailDB, kind: NodeType) -> Self {
        Self {
            db,
            name: Cow::Borrowed(""),
            prefix: Cow::Borrowed(""),
            postfix: Cow::Borrowed(""),
            delimiter: Cow::Borrowed(NameHierarchy::NAME_DELIMITER_CXX),
            parent_id: None,
            is_indexed: true,
            node_type: kind,
        }
    }

    pub fn set_name(&mut self, name: impl Into<Cow<'b, str>>) {
        self.name = name.into();
    }

    pub fn name(mut self, name: impl Into<Cow<'b, str>>) -> Self {
        self.set_name(name);
        self
    }

    pub fn set_prefix(&mut self, prefix: impl Into<Cow<'b, str>>) {
        self.prefix = prefix.into();
    }

    pub fn prefix(mut self, prefix: impl Into<Cow<'b, str>>) -> Self {
        self.set_prefix(prefix);
        self
    }

    pub fn set_postfix(&mut self, postfix: impl Into<Cow<'b, str>>) {
        self.postfix = postfix.into();
    }

    pub fn postfix(mut self, postfix: impl Into<Cow<'b, str>>) -> Self {
        self.set_postfix(postfix);
        self
    }

    pub fn set_delimiter(&mut self, delimiter: impl Into<Cow<'b, str>>) {
        self.delimiter = delimiter.into();
    }

    pub fn delimiter(mut self, delimiter: impl Into<Cow<'b, str>>) -> Self {
        self.set_delimiter(delimiter);
        self
    }

    pub fn set_parent(&mut self, id: impl Into<Option<i64>>) {
        self.parent_id = id.into();
    }

    pub fn parent(mut self, id: impl Into<Option<i64>>) -> Self {
        self.set_parent(id);
        self
    }

    pub fn set_indexed(&mut self, indexed: bool) {
        self.is_indexed = indexed;
    }

    pub fn indexed(mut self, indexed: bool) -> Self {
        self.set_indexed(indexed);
        self
    }

    pub async fn commit(self) -> Result<i64, SourcetrailError> {
        self.db
            .full_record_node(
                self.name,
                self.prefix,
                self.postfix,
                self.delimiter,
                self.parent_id,
                self.is_indexed,
                self.node_type,
            )
            .await
    }
}

pub struct SourceLocationRecorder<'a> {
    db: &'a mut SourcetrailDB,
    symbol_id: i64,
    file_id: i64,
    start_line: i32,
    start_column: i32,
    end_line: i32,
    end_column: i32,
    location_kind: SourceLocationType,
}

impl<'a> SourceLocationRecorder<'a> {
    pub fn new(db: &'a mut SourcetrailDB, kind: SourceLocationType) -> Self {
        Self {
            db,
            symbol_id: -1,
            file_id: -1,
            start_line: -1,
            start_column: -1,
            end_line: -1,
            end_column: -1,
            location_kind: kind,
        }
    }

    pub fn set_symbol(&mut self, id: i64) {
        self.symbol_id = id;
    }

    pub fn symbol(mut self, id: i64) -> Self {
        self.set_symbol(id);
        self
    }

    pub fn set_file(&mut self, id: i64) {
        self.file_id = id;
    }

    pub fn file(mut self, id: i64) -> Self {
        self.set_file(id);
        self
    }

    pub fn set_start_position(&mut self, line: i32, column: i32) {
        self.start_line = line;
        self.start_column = column;
    }

    pub fn start_position(mut self, line: i32, column: i32) -> Self {
        self.set_start_position(line, column);
        self
    }

    pub fn set_end_position(&mut self, line: i32, column: i32) {
        self.end_line = line;
        self.end_column = column;
    }

    pub fn end_position(mut self, line: i32, column: i32) -> Self {
        self.set_end_position(line, column);
        self
    }

    pub async fn commit(self) -> Result<(), SourcetrailError> {
        if self.symbol_id == -1 {
            return Err(SourcetrailError::source_location("missing symbol"));
        }

        if self.file_id == -1 {
            return Err(SourcetrailError::source_location("missing file"));
        }

        if self.start_line == -1 || self.start_column == -1 {
            return Err(SourcetrailError::source_location("missing start position"));
        }

        if self.end_line == -1 || self.end_column == -1 {
            return Err(SourcetrailError::source_location("missing end position"));
        }

        if self.start_line > self.end_line
            || self.start_line == self.end_line && self.start_column >= self.end_column
        {
            return Err(SourcetrailError::source_location("invalid source range"));
        }

        self.db
            .record_source_location(
                self.symbol_id,
                self.file_id,
                self.start_line,
                self.start_column,
                self.end_line,
                self.end_column,
                self.location_kind,
            )
            .await
    }
}

pub struct UnsolvedSymbolRecorder<'a> {
    db: &'a mut SourcetrailDB,
    symbol_id: i64,
    reference_type: Option<EdgeType>,
    file_id: i64,
    start_line: i32,
    start_column: i32,
    end_line: i32,
    end_column: i32,
}

impl<'a> UnsolvedSymbolRecorder<'a> {
    pub fn new(db: &'a mut SourcetrailDB) -> Self {
        Self {
            db,
            symbol_id: -1,
            reference_type: None,
            file_id: -1,
            start_line: -1,
            start_column: -1,
            end_line: -1,
            end_column: -1,
        }
    }

    pub fn set_symbol(&mut self, id: i64) {
        self.symbol_id = id;
    }

    pub fn symbol(mut self, id: i64) -> Self {
        self.set_symbol(id);
        self
    }

    pub fn set_reference_type(&mut self, kind: EdgeType) {
        self.reference_type = Some(kind);
    }

    pub fn reference_type(mut self, kind: EdgeType) -> Self {
        self.set_reference_type(kind);
        self
    }

    pub fn set_file(&mut self, id: i64) {
        self.file_id = id;
    }

    pub fn file(mut self, id: i64) -> Self {
        self.set_file(id);
        self
    }

    pub fn set_start_position(&mut self, line: i32, column: i32) {
        self.start_line = line;
        self.start_column = column;
    }

    pub fn start_position(mut self, line: i32, column: i32) -> Self {
        self.set_start_position(line, column);
        self
    }

    pub fn set_end_position(&mut self, line: i32, column: i32) {
        self.end_line = line;
        self.end_column = column;
    }

    pub fn end_position(mut self, line: i32, column: i32) -> Self {
        self.set_end_position(line, column);
        self
    }

    pub async fn commit(self) -> Result<i64, SourcetrailError> {
        if self.symbol_id == -1 {
            return Err(SourcetrailError::unsolved_symbol("missing symbol"));
        }

        if self.file_id == -1 {
            return Err(SourcetrailError::unsolved_symbol("missing file"));
        }

        if self.start_line == -1 || self.start_column == -1 {
            return Err(SourcetrailError::unsolved_symbol("missing start position"));
        }

        if self.end_line == -1 || self.end_column == -1 {
            return Err(SourcetrailError::unsolved_symbol("missing end position"));
        }

        if self.start_line > self.end_line
            || self.start_line == self.end_line && self.start_column >= self.end_column
        {
            return Err(SourcetrailError::unsolved_symbol("invalid source range"));
        }

        let reference_type = self
            .reference_type
            .ok_or_else(|| SourcetrailError::unsolved_symbol("missing reference type"))?;

        let hierarchy = NameHierarchy::new(
            NameHierarchy::NAME_DELIMITER_UNKNOWN,
            [NameElementBuilder::new().name("unsolved symbol").build()],
        )?;

        let unsolved_symbol_id = self.db.record_symbol(&hierarchy).await?;

        let elem_id = ElementDAO::new(&self.db.database).await?;
        let reference_id = EdgeDAO::new(
            &self.db.database,
            Edge::new(elem_id, reference_type, self.symbol_id, unsolved_symbol_id),
        )
        .await?;

        self.db
            .record_source_location(
                reference_id,
                self.file_id,
                self.start_line,
                self.start_column,
                self.end_line,
                self.end_column,
                SourceLocationType::Unsolved,
            )
            .await?;

        Ok(reference_id)
    }
}

pub struct ErrorRecorder<'a, 'b> {
    db: &'a mut SourcetrailDB,
    msg: Cow<'b, str>,
    fatal: bool,
    file_id: i64,
    start_line: i32,
    start_column: i32,
    end_line: i32,
    end_column: i32,
}

impl<'a, 'b> ErrorRecorder<'a, 'b> {
    pub fn new(db: &'a mut SourcetrailDB) -> Self {
        Self {
            db,
            msg: Cow::Borrowed(""),
            fatal: false,
            file_id: -1,
            start_line: -1,
            start_column: -1,
            end_line: -1,
            end_column: -1,
        }
    }

    pub fn set_message(&mut self, msg: impl Into<Cow<'b, str>>) {
        self.msg = msg.into();
    }

    pub fn message(mut self, msg: impl Into<Cow<'b, str>>) -> Self {
        self.set_message(msg);
        self
    }

    pub fn set_fatal(&mut self, fatal: bool) {
        self.fatal = fatal;
    }

    pub fn fatal(mut self, fatal: bool) -> Self {
        self.set_fatal(fatal);
        self
    }

    pub fn set_file(&mut self, id: i64) {
        self.file_id = id;
    }

    pub fn file(mut self, id: i64) -> Self {
        self.set_file(id);
        self
    }

    pub fn set_start_position(&mut self, line: i32, column: i32) {
        self.start_line = line;
        self.start_column = column;
    }

    pub fn start_position(mut self, line: i32, column: i32) -> Self {
        self.set_start_position(line, column);
        self
    }

    pub fn set_end_position(&mut self, line: i32, column: i32) {
        self.end_line = line;
        self.end_column = column;
    }

    pub fn end_position(mut self, line: i32, column: i32) -> Self {
        self.set_end_position(line, column);
        self
    }

    pub async fn commit(self) -> Result<(), SourcetrailError> {
        if self.file_id == -1 {
            return Err(SourcetrailError::error_location("missing file"));
        }

        if self.msg.is_empty() {
            return Err(SourcetrailError::error_location("message is empty"));
        }

        if self.start_line == -1 || self.start_column == -1 {
            return Err(SourcetrailError::error_location("missing start position"));
        }

        if self.end_line == -1 || self.end_column == -1 {
            return Err(SourcetrailError::error_location("missing end position"));
        }

        if self.start_line > self.end_line
            || self.start_line == self.end_line && self.start_column >= self.end_column
        {
            return Err(SourcetrailError::error_location("invalid source range"));
        }

        let elem_id = ElementDAO::new(&self.db.database).await?;
        ErrorDAO::new(
            &self.db.database,
            Error::new(elem_id, self.msg, self.fatal, true, ""),
        )
        .await?;

        self.db
            .record_source_location(
                elem_id,
                self.file_id,
                self.start_line,
                self.start_column,
                self.end_line,
                self.end_column,
                SourceLocationType::IndexerError,
            )
            .await?;
        Ok(())
    }
}

pub struct FileRecorder<'a, 'b> {
    db: &'a mut SourcetrailDB,
    path: Option<PathBuf>,
    modification_time: DateTime<Utc>,
    content: Cow<'b, str>,
    indexed: bool,
}

impl<'a, 'b> FileRecorder<'a, 'b> {
    pub fn new(db: &'a mut SourcetrailDB) -> Self {
        Self {
            db,
            path: None,
            modification_time: chrono::offset::Utc::now(),
            content: Cow::Borrowed(""),
            indexed: true,
        }
    }

    pub fn set_path(&mut self, path: impl Into<PathBuf>) {
        self.path = Some(path.into());
    }

    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.set_path(path);
        self
    }

    pub fn set_modification_time(&mut self, time: impl Into<DateTime<Utc>>) {
        self.modification_time = time.into();
    }

    pub fn modification_time(mut self, time: impl Into<DateTime<Utc>>) -> Self {
        self.set_modification_time(time);
        self
    }

    pub fn set_content(&mut self, content: impl Into<Cow<'b, str>>) {
        self.content = content.into();
    }

    pub fn content(mut self, content: impl Into<Cow<'b, str>>) -> Self {
        self.set_content(content);
        self
    }

    pub fn set_indexed(&mut self, indexed: bool) {
        self.indexed = indexed;
    }

    pub fn indexed(mut self, indexed: bool) -> Self {
        self.set_indexed(indexed);
        self
    }

    pub async fn commit_file(self, path: impl AsRef<Path>) -> Result<i64, SourcetrailError> {
        let path = path.as_ref();
        let mut file = async_std::fs::File::open(path)
            .await
            .map_err(SourcetrailError::file_recorder)?;

        let meta = file
            .metadata()
            .await
            .map_err(SourcetrailError::file_recorder)?;

        let modified = meta.modified().map_err(SourcetrailError::file_recorder)?;

        let mut buf = String::with_capacity(meta.len() as _);
        file.read_to_string(&mut buf)
            .await
            .map_err(SourcetrailError::file_recorder)?;

        self.path(path)
            .modification_time(modified)
            .content(buf)
            .commit()
            .await
    }

    pub async fn commit(self) -> Result<i64, SourcetrailError> {
        let path = self
            .path
            .ok_or_else(|| SourcetrailError::file_recorder_with("missing file path"))?;

        self.db
            .record_file_with(path, self.modification_time, self.content, self.indexed)
            .await
    }
}

pub struct SourcetrailDB {
    database: SqlitePool,
    path: PathBuf,
    name_cache: HashMap<String, i64>,
}

impl SourcetrailDB {
    const SOURCETRAIL_PROJECT_EXT: &'static str = "srctrlprj";
    const SOURCETRAIL_DB_EXT: &'static str = "srctrldb";
    const SOURCETRAIL_XML: &'static str = r#"<?xml version="1.0" encoding="utf-8"?>
<config>
    <version>0</version>
</config>"#;

    pub fn new(database: SqlitePool, path: PathBuf) -> Self {
        SourcetrailDB {
            database,
            path,
            name_cache: HashMap::new(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn uniformize_path(path: &Path) -> PathBuf {
        let mut path = path.to_path_buf();
        if path.extension().unwrap_or_default() != Self::SOURCETRAIL_DB_EXT {
            path.set_extension(Self::SOURCETRAIL_DB_EXT);
        }
        path
    }

    pub fn exists(path: impl AsRef<Path>) -> bool {
        Self::uniformize_path(path.as_ref()).exists()
    }

    pub async fn open(path: impl AsRef<Path>, clear: bool) -> Result<Self, SourcetrailError> {
        let path = Self::uniformize_path(path.as_ref());
        if !path.exists() {
            if !clear {
                return Err(SourcetrailError::File(format!(
                    "{} not found",
                    path.display()
                )));
            }
            return Self::create(&path).await;
        }

        let database = SqliteHelper::connect(path.to_string_lossy().as_ref()).await?;
        let db = SourcetrailDB::new(database, path);

        if clear {
            db.clear().await?;
        }

        Ok(db)
    }

    pub async fn create(path: impl AsRef<Path>) -> Result<Self, SourcetrailError> {
        let path = Self::uniformize_path(path.as_ref());
        if path.exists() {
            return Err(SourcetrailError::File(format!(
                "{} already exists",
                path.display()
            )));
        }

        let database = SqliteHelper::connect(path.to_string_lossy().as_ref()).await?;
        let db = SourcetrailDB::new(database, path.clone());

        db.create_sql_tables().await?;

        MetaDAO::new(&db.database, "storage_version", "25").await?;
        MetaDAO::new(&db.database, "project_settings", Self::SOURCETRAIL_XML).await?;

        let project_file = path.with_extension(Self::SOURCETRAIL_PROJECT_EXT);
        fs::write(&project_file, Self::SOURCETRAIL_XML)?;

        Ok(db)
    }

    async fn create_sql_tables(&self) -> Result<(), SourcetrailError> {
        ElementDAO::create_table(&self.database).await?;
        ElementComponentDAO::create_table(&self.database).await?;
        EdgeDAO::create_table(&self.database).await?;
        NodeDAO::create_table(&self.database).await?;
        SymbolDAO::create_table(&self.database).await?;
        FileDAO::create_table(&self.database).await?;
        FileContentDAO::create_table(&self.database).await?;
        LocalSymbolDAO::create_table(&self.database).await?;
        SourceLocationDAO::create_table(&self.database).await?;
        OccurrenceDAO::create_table(&self.database).await?;
        ComponentAccessDAO::create_table(&self.database).await?;
        ErrorDAO::create_table(&self.database).await?;
        MetaDAO::create_table(&self.database).await?;
        Ok(())
    }

    pub async fn clear(&self) -> Result<(), SourcetrailError> {
        ElementDAO::clear(&self.database).await?;
        ElementComponentDAO::clear(&self.database).await?;
        EdgeDAO::clear(&self.database).await?;
        NodeDAO::clear(&self.database).await?;
        SymbolDAO::clear(&self.database).await?;
        FileDAO::clear(&self.database).await?;
        FileContentDAO::clear(&self.database).await?;
        LocalSymbolDAO::clear(&self.database).await?;
        SourceLocationDAO::clear(&self.database).await?;
        OccurrenceDAO::clear(&self.database).await?;
        ComponentAccessDAO::clear(&self.database).await?;
        ErrorDAO::clear(&self.database).await?;
        Ok(())
    }

    pub async fn close(&self) -> Result<(), SourcetrailError> {
        self.database.close().await;
        Ok(())
    }

    async fn add_if_not_existing(
        &mut self,
        name: impl AsRef<str>,
        type_: NodeType,
    ) -> Result<i64, SourcetrailError> {
        let name = name.as_ref();

        if !self.name_cache.contains_key(name) {
            let elem_id = ElementDAO::new(&self.database).await?;
            NodeDAO::new(&self.database, &Node::new(elem_id, type_, name)).await?;
            self.name_cache.insert(name.to_owned(), elem_id);
            Ok(elem_id)
        } else {
            Ok(*self.name_cache.get(name).expect("exists"))
        }
    }

    pub fn record_node<'a, 'b>(&'a mut self, kind: NodeType) -> NodeRecorder<'a, 'b> {
        NodeRecorder::new(self, kind)
    }

    async fn record_symbol(&mut self, hierarchy: &NameHierarchy) -> Result<i64, SourcetrailError> {
        let mut ids = vec![];
        for i in 0..hierarchy.size() {
            ids.push(
                self.add_if_not_existing(
                    &hierarchy.serialize_range(0, i + 1)?,
                    NodeType::NodeSymbol,
                )
                .await?,
            );
        }

        for pair in ids.windows(2) {
            let parent = pair[0];
            let child = pair[1];
            let elem_id = ElementDAO::new(&self.database).await?;
            EdgeDAO::new(
                &self.database,
                Edge::new(elem_id, EdgeType::Member, parent, child),
            )
            .await?;
        }

        Ok(*ids.last().expect("at least one id"))
    }

    async fn full_record_node(
        &mut self,
        name: impl AsRef<str>,
        prefix: impl AsRef<str>,
        postfix: impl AsRef<str>,
        delimiter: impl AsRef<str>,
        parent_id: impl Into<Option<i64>>,
        is_indexed: bool,
        node_type: NodeType,
    ) -> Result<i64, SourcetrailError> {
        let name_element = NameElement::builder()
            .prefix(prefix.as_ref())
            .name(name.as_ref())
            .postfix(postfix.as_ref())
            .build();

        let obj_id = if let Some(parent_id) = parent_id.into() {
            let node = NodeDAO::get(&self.database, parent_id)
                .await?
                .ok_or(SourcetrailError::ParentNotFound(parent_id))?;
            let mut hierarchy = NameHierarchy::deserialize_name(node.name())?;
            hierarchy.push_element(name_element);
            self.record_symbol(&hierarchy).await?
        } else {
            self.record_symbol(&NameHierarchy::new(delimiter.as_ref(), [name_element])?)
                .await?
        };

        self.record_symbol_kind(obj_id, node_type).await?;

        if is_indexed {
            self.record_symbol_definition_kind(obj_id, SymbolType::Explicit)
                .await?;
        }

        Ok(obj_id)
    }

    async fn record_symbol_kind(&self, id: i64, type_: NodeType) -> Result<(), SourcetrailError> {
        if let Some(mut node) = NodeDAO::get(&self.database, id).await? {
            node.set_type(type_);
            NodeDAO::update(&self.database, node).await?;
        }
        Ok(())
    }

    async fn record_symbol_definition_kind(
        &self,
        id: i64,
        kind: SymbolType,
    ) -> Result<(), SourcetrailError> {
        if let Some(mut sym) = SymbolDAO::get(&self.database, id).await? {
            if sym.definition_kind() != kind {
                sym.set_definition_kind(kind);
                SymbolDAO::update(&self.database, sym).await?;
            }
        } else {
            SymbolDAO::new(&self.database, Symbol::new(id, kind)).await?;
        }
        Ok(())
    }

    pub fn record_symbol_node<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeSymbol)
    }

    pub fn record_builtin_type_node<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeBuiltinType)
    }

    pub fn record_module<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeModule)
    }

    pub fn record_namespace<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeNamespace)
    }

    pub fn record_package<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodePackage)
    }

    pub fn record_struct<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeStruct)
    }

    pub fn record_class<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeClass)
    }

    pub fn record_interface<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeInterface)
    }

    pub fn record_annotation<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeAnnotation)
    }

    pub fn record_global_variable<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeGlobalVariable)
    }

    pub fn record_field<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeField)
    }

    pub fn record_function<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeFunction)
    }

    pub fn record_method<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeMethod)
    }

    pub fn record_enum<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeEnum)
    }

    pub fn record_enum_constant<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeEnumConstant)
    }

    pub fn record_typedef_node<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeTypedef)
    }

    pub fn record_type_parameter_node<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeTypeParameter)
    }

    pub fn record_type_node<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeType)
    }

    pub fn record_macro<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeMacro)
    }

    pub fn record_union<'a, 'b>(&'a mut self) -> NodeRecorder<'a, 'b> {
        self.record_node(NodeType::NodeUnion)
    }

    async fn record_reference(
        &mut self,
        source_id: i64,
        target_id: i64,
        edge_type: EdgeType,
    ) -> Result<i64, SourcetrailError> {
        let elem_id = ElementDAO::new(&self.database).await?;
        EdgeDAO::new(
            &self.database,
            Edge::new(elem_id, edge_type, source_id, target_id),
        )
        .await?;
        Ok(elem_id)
    }

    pub async fn record_ref_member(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::Member)
            .await
    }

    pub async fn record_ref_type_usage(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::TypeUsage)
            .await
    }

    pub async fn record_ref_usage(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::Usage)
            .await
    }

    pub async fn record_ref_call(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::Call)
            .await
    }

    pub async fn record_ref_inheritance(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::Inheritance)
            .await
    }

    pub async fn record_ref_override(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::Override)
            .await
    }

    pub async fn record_ref_type_argument(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::TypeArgument)
            .await
    }

    pub async fn record_ref_template_specialization(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::TemplateSpecialization)
            .await
    }

    pub async fn record_ref_include(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::Include)
            .await
    }

    pub async fn record_ref_import(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::Import)
            .await
    }

    pub async fn record_ref_bundled_edges(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::BundledEdges)
            .await
    }

    pub async fn record_ref_macro_usage(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::MacroUsage)
            .await
    }

    pub async fn record_ref_annotation_usage(
        &mut self,
        source_id: i64,
        target_id: i64,
    ) -> Result<i64, SourcetrailError> {
        self.record_reference(source_id, target_id, EdgeType::AnnotationUsage)
            .await
    }

    pub fn record_reference_to_unsolved_symbol<'a>(&'a mut self) -> UnsolvedSymbolRecorder<'a> {
        UnsolvedSymbolRecorder::new(self)
    }

    pub async fn record_reference_is_ambiguous(
        &mut self,
        reference_id: i64,
    ) -> Result<(), SourcetrailError> {
        ElementComponentDAO::new(
            &self.database,
            ElementComponent::new(0, reference_id, ElementComponentType::IsAmbiguous, ""),
        )
        .await?;
        Ok(())
    }

    pub fn record_file<'a, 'b>(&'a mut self) -> FileRecorder<'a, 'b> {
        FileRecorder::new(self)
    }

    async fn record_file_with(
        &mut self,
        path: impl AsRef<Path>,
        modification_time: DateTime<Utc>,
        content: impl AsRef<str>,
        indexed: bool,
    ) -> Result<i64, SourcetrailError> {
        let path = path.as_ref();

        let hierarchy = NameHierarchy::new(
            NameHierarchy::NAME_DELIMITER_FILE,
            [NameElement::builder().name(path.to_string_lossy()).build()],
        )?;

        let content = content.as_ref();

        let lines = if indexed {
            content.lines().count() as u32
        } else {
            0
        };

        let elem_id = self
            .add_if_not_existing(hierarchy.serialize_name()?, NodeType::NodeFile)
            .await?;

        FileDAO::new(
            &self.database,
            File::builder()
                .id(elem_id)
                .path(path)
                .modification_time(modification_time)
                .indexed(indexed)
                .complete(true)
                .line_count(lines)
                .build()
                .map_err(SourcetrailError::builder)?,
        )
        .await?;

        if indexed {
            FileContentDAO::new(&self.database, FileContent::new(elem_id, content)).await?;
        }

        Ok(elem_id)
    }

    pub async fn record_file_language(
        &mut self,
        id: i64,
        language: impl AsRef<str>,
    ) -> Result<(), SourcetrailError> {
        let mut file = FileDAO::get(&self.database, id)
            .await?
            .ok_or(SourcetrailError::FileNotFound(id))?;
        file.set_language(language.as_ref());
        FileDAO::update(&self.database, file).await?;
        Ok(())
    }

    async fn record_source_location(
        &mut self,
        symbol_id: i64,
        file_id: i64,
        start_line: i32,
        start_column: i32,
        end_line: i32,
        end_column: i32,
        location_type: SourceLocationType,
    ) -> Result<(), SourcetrailError> {
        let loc_id = SourceLocationDAO::new(
            &self.database,
            SourceLocation::new(
                0,
                file_id,
                start_line,
                start_column,
                end_line,
                end_column,
                location_type,
            )?,
        )
        .await?;

        OccurrenceDAO::new(&self.database, Occurrence::new(symbol_id, loc_id)).await?;

        Ok(())
    }

    pub fn record_location<'a>(
        &'a mut self,
        kind: SourceLocationType,
    ) -> SourceLocationRecorder<'a> {
        SourceLocationRecorder::new(self, kind)
    }

    pub fn record_symbol_location<'a>(&'a mut self) -> SourceLocationRecorder<'a> {
        self.record_location(SourceLocationType::Token)
    }

    pub fn record_symbol_scope_location<'a>(&'a mut self) -> SourceLocationRecorder<'a> {
        self.record_location(SourceLocationType::Scope)
    }

    pub fn record_symbol_signature_location<'a>(&'a mut self) -> SourceLocationRecorder<'a> {
        self.record_location(SourceLocationType::Signature)
    }

    pub fn record_reference_location<'a>(&'a mut self) -> SourceLocationRecorder<'a> {
        self.record_location(SourceLocationType::Token)
    }

    pub fn record_qualifier_location<'a>(&'a mut self) -> SourceLocationRecorder<'a> {
        self.record_location(SourceLocationType::Qualifier)
    }

    pub async fn record_local_symbol(
        &mut self,
        name: impl AsRef<str>,
    ) -> Result<i64, SourcetrailError> {
        let name = name.as_ref();
        if let Some(local) = LocalSymbolDAO::get_by_name(&self.database, name).await? {
            return Ok(local.id());
        }

        let elem_id = ElementDAO::new(&self.database).await?;
        LocalSymbolDAO::new(&self.database, LocalSymbol::new(elem_id, name)).await?;
        Ok(elem_id)
    }

    pub fn record_local_symbol_location<'a>(&'a mut self) -> SourceLocationRecorder<'a> {
        self.record_location(SourceLocationType::LocalSymbol)
    }

    pub fn record_atomic_source_range<'a>(&'a mut self) -> SourceLocationRecorder<'a> {
        self.record_location(SourceLocationType::AtomicRange)
    }

    pub fn record_error<'a, 'b>(&'a mut self) -> ErrorRecorder<'a, 'b> {
        ErrorRecorder::new(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[async_std::test]
    #[ignore]
    async fn test_db() -> Result<(), Box<dyn std::error::Error>> {
        let mut db = SourcetrailDB::create("test").await?;

        let my_main = db.record_class().name("MyMainClass").commit().await?;

        let meth_id = db
            .record_method()
            .name("main")
            .parent(my_main)
            .commit()
            .await?;

        let class_id = db.record_class().name("PersonalInfo").commit().await?;

        let field_id = db
            .record_field()
            .name("first_name")
            .parent(class_id)
            .commit()
            .await?;

        db.record_ref_usage(meth_id, field_id).await?;

        Ok(())
    }
}
