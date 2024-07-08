use chrono::NaiveDateTime;
use sqlx::sqlite::{SqliteArguments, SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{FromRow, SqlitePool};

use crate::api::SourcetrailError;
use crate::types::{
    Edge as EdgeRepr, Element as ElementRepr, ElementComponent as ElementComponentRepr,
    Error as ErrorRepr, File as FileRepr, FileContent as FileContentRepr,
    LocalSymbol as LocalSymbolRepr, Meta as MetaRepr, Node as NodeRepr,
    Occurrence as OccurrenceRepr, SourceLocation as SourceLocationRepr, Symbol as SymbolRepr,
};

macro_rules! query_args {
    ( $( $arg:expr ),* ) => {
        {
            #[allow(unused)]
            use sqlx::Arguments;

            #[allow(unused_mut)]
            let mut container = SqliteArguments::default();
            $(
                container.add($arg);
            )*
            container
        }
    };
}

pub struct SqliteHelper;

impl SqliteHelper {
    pub async fn connect(path: &str) -> Result<SqlitePool, SourcetrailError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(
                SqliteConnectOptions::default()
                    .create_if_missing(true)
                    .filename(path),
            )
            .await?;
        Ok(pool)
    }

    pub async fn exec(
        pool: &SqlitePool,
        query: &str,
        params: SqliteArguments<'_>,
    ) -> Result<i64, SourcetrailError> {
        let mut tx = pool.begin().await?;
        let res = sqlx::query_with(query, params).execute(tx.as_mut()).await?;
        tx.commit().await?;
        Ok(res.last_insert_rowid())
    }

    pub async fn fetch_one<T>(
        pool: &SqlitePool,
        query: &str,
        params: SqliteArguments<'_>,
    ) -> Result<Option<T>, SourcetrailError>
    where
        T: for<'q> FromRow<'q, SqliteRow> + Send + Unpin,
    {
        let res = sqlx::query_as_with::<_, T, _>(query, params)
            .fetch_one(pool)
            .await;

        if matches!(res, Err(sqlx::Error::RowNotFound)) {
            Ok(None)
        } else {
            Ok(Some(res?))
        }
    }

    pub async fn fetch<T>(
        pool: &SqlitePool,
        query: &str,
        params: SqliteArguments<'_>,
    ) -> Result<Vec<T>, SourcetrailError>
    where
        T: for<'q> FromRow<'q, SqliteRow> + Send + Unpin,
    {
        let rows = sqlx::query_as_with::<_, T, _>(query, params)
            .fetch_all(pool)
            .await?;
        Ok(rows)
    }
}

#[derive(FromRow, Debug)]
struct Element {
    id: i64,
}

impl From<Element> for ElementRepr {
    fn from(elt: Element) -> Self {
        Self::new(elt.id)
    }
}

pub struct ElementDAO;

impl ElementDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "CREATE TABLE IF NOT EXISTS element(id INTEGER PRIMARY KEY);",
            query_args![],
        )
        .await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS element;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(pool: &SqlitePool) -> Result<i64, SourcetrailError> {
        SqliteHelper::exec(pool, "INSERT INTO element(id) VALUES(NULL);", query_args![]).await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM element WHERE id = ?;", query_args![&id]).await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM element;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<ElementRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<Element>(
            pool,
            "SELECT * FROM element WHERE id = ?;",
            query_args![&id],
        )
        .await?;

        Ok(result.map(ElementRepr::from))
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<ElementRepr>, SourcetrailError> {
        Ok(SqliteHelper::fetch::<Element>(
            pool,
            "SELECT * FROM element;",
            SqliteArguments::default(),
        )
        .await?
        .into_iter()
        .map(ElementRepr::from)
        .collect())
    }
}

#[derive(FromRow, Debug)]
struct ElementComponent {
    id: i64,
    element_id: i64,
    #[sqlx(rename = "type")]
    type_: i32,
    data: String,
}

impl TryFrom<ElementComponent> for ElementComponentRepr {
    type Error = SourcetrailError;

    fn try_from(elt: ElementComponent) -> Result<Self, Self::Error> {
        Ok(Self::new(
            elt.id,
            elt.element_id,
            elt.type_.try_into().map_err(SourcetrailError::convert)?,
            elt.data,
        ))
    }
}

pub struct ElementComponentDAO;

impl ElementComponentDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS element_component(id INTEGER PRIMARY KEY, element_id INTEGER, type INTEGER, data TEXT, FOREIGN KEY(element_id) REFERENCES element(id) ON DELETE CASCADE);", query_args![]).await?;

        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "DROP TABLE IF EXISTS element_component;",
            query_args![],
        )
        .await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<ElementComponentRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "INSERT INTO element_component(id, element_id, type, data) VALUES(NULL, ?, ?, ?);",
            query_args![obj.elem_id(), obj.component_type() as i32, obj.data()],
        )
        .await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "DELETE FROM element_component WHERE id = ?;",
            query_args![&id],
        )
        .await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM element_component;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(
        pool: &SqlitePool,
        id: i64,
    ) -> Result<Option<ElementComponentRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<ElementComponent>(
            pool,
            "SELECT * FROM element_component WHERE id = ?;",
            query_args![&id],
        )
        .await?;

        result.map(ElementComponentRepr::try_from).transpose()
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<ElementComponentRepr>, SourcetrailError> {
        SqliteHelper::fetch::<ElementComponent>(
            pool,
            "SELECT * FROM element_component;",
            query_args![],
        )
        .await?
        .into_iter()
        .map(ElementComponentRepr::try_from)
        .collect::<Result<_, _>>()
    }
}

#[derive(FromRow, Debug)]
struct Edge {
    id: i64,
    #[sqlx(rename = "type")]
    type_: i32,
    source_node_id: i64,
    target_node_id: i64,
}

impl TryFrom<Edge> for EdgeRepr {
    type Error = SourcetrailError;

    fn try_from(edge: Edge) -> Result<Self, Self::Error> {
        Ok(Self::new(
            edge.id,
            edge.type_.try_into().map_err(SourcetrailError::convert)?,
            edge.source_node_id,
            edge.target_node_id,
        ))
    }
}

pub struct EdgeDAO;

impl EdgeDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS edge(id INTEGER PRIMARY KEY, type INTEGER, source_node_id INTEGER, target_node_id INTEGER, FOREIGN KEY(source_node_id) REFERENCES node(id) ON DELETE CASCADE, FOREIGN KEY(target_node_id) REFERENCES node(id) ON DELETE CASCADE);", query_args![]).await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS edge;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<EdgeRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "INSERT INTO edge(id, type, source_node_id, target_node_id) VALUES(?, ?, ?, ?);",
            query_args![
                obj.id(),
                obj.type_() as i32,
                obj.source_id(),
                obj.target_id()
            ],
        )
        .await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM edge WHERE id = ?;", query_args![&id]).await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM edge;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<EdgeRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<Edge>(
            pool,
            "SELECT * FROM edge WHERE id = ?;",
            query_args![id],
        )
        .await?;

        result.map(EdgeRepr::try_from).transpose()
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<EdgeRepr>, SourcetrailError> {
        SqliteHelper::fetch::<Edge>(pool, "SELECT * FROM edge;", query_args![])
            .await?
            .into_iter()
            .map(EdgeRepr::try_from)
            .collect::<Result<_, _>>()
    }
}

#[derive(FromRow, Debug)]
struct Node {
    id: i64,
    #[sqlx(rename = "type")]
    type_: i32,
    serialized_name: String,
}

impl TryFrom<Node> for NodeRepr {
    type Error = SourcetrailError;

    fn try_from(node: Node) -> Result<Self, Self::Error> {
        Ok(Self::new(
            node.id,
            node.type_.try_into().map_err(SourcetrailError::convert)?,
            node.serialized_name,
        ))
    }
}

pub struct NodeDAO;

impl NodeDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS node(id INTEGER PRIMARY KEY, type INTEGER, serialized_name TEXT, FOREIGN KEY(id) REFERENCES element(id) ON DELETE CASCADE);", query_args![]).await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS node;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<NodeRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "INSERT INTO node(id, type, serialized_name) VALUES(?, ?, ?);",
            query_args![obj.id(), obj.type_() as i32, obj.name()],
        )
        .await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM node WHERE id = ?;", query_args![&id]).await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM node;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<NodeRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<Node>(
            pool,
            "SELECT * FROM node WHERE id = ?;",
            query_args![&id],
        )
        .await?;

        result.map(NodeRepr::try_from).transpose()
    }

    pub async fn get_by_name(
        pool: &SqlitePool,
        name: impl AsRef<str>,
    ) -> Result<Option<NodeRepr>, SourcetrailError> {
        let name = name.as_ref();
        let result = SqliteHelper::fetch_one::<Node>(
            pool,
            "SELECT * FROM node WHERE serialized_name = ? LIMIT 1;",
            query_args![&name],
        )
        .await?;

        result.map(NodeRepr::try_from).transpose()
    }

    pub async fn update(
        pool: &SqlitePool,
        obj: impl AsRef<NodeRepr>,
    ) -> Result<(), SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "UPDATE node SET type = ?, serialized_name = ? WHERE id = ?;",
            query_args![obj.type_() as i32, obj.name(), obj.id()],
        )
        .await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<NodeRepr>, SourcetrailError> {
        SqliteHelper::fetch::<Node>(pool, "SELECT * FROM node;", query_args![])
            .await?
            .into_iter()
            .map(NodeRepr::try_from)
            .collect::<Result<_, _>>()
    }
}

#[derive(FromRow, Debug)]
struct Symbol {
    id: i64,
    definition_kind: i32,
}

impl TryFrom<Symbol> for SymbolRepr {
    type Error = SourcetrailError;

    fn try_from(sym: Symbol) -> Result<Self, Self::Error> {
        Ok(Self::new(
            sym.id,
            sym.definition_kind
                .try_into()
                .map_err(SourcetrailError::convert)?,
        ))
    }
}

pub struct SymbolDAO;

impl SymbolDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS symbol(id INTEGER PRIMARY KEY, definition_kind INTEGER, FOREIGN KEY(id) REFERENCES node(id) ON DELETE CASCADE);", query_args![]).await?;

        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS symbol;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<SymbolRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "INSERT INTO symbol(id, definition_kind) VALUES(?, ?);",
            query_args![obj.id(), obj.definition_kind() as i32],
        )
        .await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM symbol WHERE id = ?;", query_args![&id]).await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM symbol;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<SymbolRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<Symbol>(
            pool,
            "SELECT * FROM symbol WHERE id = ?;",
            query_args![id],
        )
        .await?;

        result.map(SymbolRepr::try_from).transpose()
    }

    pub async fn update(
        pool: &SqlitePool,
        obj: impl AsRef<SymbolRepr>,
    ) -> Result<(), SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "UPDATE symbol SET definition_kind = ? WHERE id = ?;",
            query_args![obj.definition_kind() as i32, obj.id()],
        )
        .await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<SymbolRepr>, SourcetrailError> {
        SqliteHelper::fetch::<Symbol>(pool, "SELECT * FROM symbol;", query_args![])
            .await?
            .into_iter()
            .map(SymbolRepr::try_from)
            .collect::<Result<_, _>>()
    }
}

#[derive(FromRow, Debug)]
struct File {
    id: i64,
    path: String,
    language: String,
    modification_time: String,
    indexed: bool,
    complete: bool,
    line_count: u32,
}

impl TryFrom<File> for FileRepr {
    type Error = SourcetrailError;

    fn try_from(f: File) -> Result<Self, Self::Error> {
        Ok(Self::new(
            f.id,
            f.path,
            f.language,
            NaiveDateTime::parse_from_str("%Y-%m-%d %H:%M:%S", &f.modification_time)
                .map_err(SourcetrailError::convert)?
                .and_utc(),
            f.indexed,
            f.complete,
            f.line_count,
        ))
    }
}

pub struct FileDAO;

impl FileDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS file(id INTEGER PRIMARY KEY, path TEXT, language TEXT, modification_time TEXT, indexed BOOLEAN, complete BOOLEAN, line_count INTEGER, FOREIGN KEY(id) REFERENCES node(id) ON DELETE CASCADE);", query_args![]).await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS file;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<FileRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(pool, "INSERT INTO file(id, path, language, modification_time, indexed, complete, line_count) VALUES(?, ?, ?, ?, ?, ?, ?);", query_args![obj.id(), obj.path().to_string_lossy(), obj.language(), obj.modification_time_str(), obj.is_indexed(), obj.is_complete(), obj.line_count()]).await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM file WHERE id = ?;", query_args![&id]).await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM file;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<FileRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<File>(
            pool,
            "SELECT * FROM file WHERE id = ?;",
            query_args![&id],
        )
        .await?;
        result.map(FileRepr::try_from).transpose()
    }

    pub async fn update(
        pool: &SqlitePool,
        obj: impl AsRef<FileRepr>,
    ) -> Result<(), SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(pool, "UPDATE file SET path = ?, language = ?, modification_time = ?, indexed = ?, complete = ?, line_count = ? WHERE id = ?;", query_args![obj.path().to_string_lossy(), obj.language(), obj.modification_time().to_rfc3339(), obj.is_indexed(), obj.is_complete(), obj.line_count(), obj.id()]).await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<FileRepr>, SourcetrailError> {
        SqliteHelper::fetch::<File>(pool, "SELECT * FROM file;", query_args![])
            .await?
            .into_iter()
            .map(FileRepr::try_from)
            .collect::<Result<_, _>>()
    }
}

#[derive(FromRow, Debug)]
struct FileContent {
    id: i64,
    content: String,
}

impl From<FileContent> for FileContentRepr {
    fn from(content: FileContent) -> Self {
        Self::new(content.id, content.content)
    }
}

pub struct FileContentDAO;

impl FileContentDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS filecontent(id INTEGER PRIMARY KEY, content TEXT, FOREIGN KEY(id) REFERENCES file(id) ON DELETE CASCADE);", query_args![]).await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS filecontent;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<FileContentRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "INSERT INTO filecontent(id, content) VALUES(?, ?);",
            query_args![obj.id(), obj.content()],
        )
        .await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "DELETE FROM filecontent WHERE id = ?;",
            query_args![&id],
        )
        .await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM filecontent;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(
        pool: &SqlitePool,
        id: i64,
    ) -> Result<Option<FileContentRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<FileContent>(
            pool,
            "SELECT * FROM filecontent WHERE id = ?;",
            query_args![&id],
        )
        .await?;

        Ok(result.map(FileContentRepr::from))
    }

    pub async fn update(
        pool: &SqlitePool,
        obj: impl AsRef<FileContentRepr>,
    ) -> Result<(), SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "UPDATE filecontent SET content = ? WHERE id = ?;",
            query_args![obj.content(), obj.id()],
        )
        .await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<FileContentRepr>, SourcetrailError> {
        Ok(
            SqliteHelper::fetch::<FileContent>(pool, "SELECT * FROM filecontent;", query_args![])
                .await?
                .into_iter()
                .map(FileContentRepr::from)
                .collect(),
        )
    }
}

#[derive(FromRow, Debug)]
struct LocalSymbol {
    id: i64,
    name: String,
}

impl From<LocalSymbol> for LocalSymbolRepr {
    fn from(sym: LocalSymbol) -> Self {
        Self::new(sym.id, sym.name)
    }
}

pub struct LocalSymbolDAO;

impl LocalSymbolDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS local_symbol(id INTEGER PRIMARY KEY, name TEXT, FOREIGN KEY(id) REFERENCES element(id) ON DELETE CASCADE);", query_args![]).await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS local_symbol;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<LocalSymbolRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "INSERT INTO local_symbol(id, name) VALUES(?, ?);",
            query_args![obj.id(), obj.name()],
        )
        .await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "DELETE FROM local_symbol WHERE id = ?;",
            query_args![id],
        )
        .await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM local_symbol;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(
        pool: &SqlitePool,
        id: i64,
    ) -> Result<Option<LocalSymbolRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<LocalSymbol>(
            pool,
            "SELECT * FROM local_symbol WHERE id = ?;",
            query_args![&id],
        )
        .await?;

        Ok(result.map(LocalSymbolRepr::from))
    }

    pub async fn get_by_name(
        pool: &SqlitePool,
        name: impl AsRef<str>,
    ) -> Result<Option<LocalSymbolRepr>, SourcetrailError> {
        let name = name.as_ref();
        let result = SqliteHelper::fetch_one::<LocalSymbol>(
            pool,
            "SELECT * FROM local_symbol WHERE name = ? LIMIT 1;",
            query_args![&name],
        )
        .await?;
        Ok(result.map(LocalSymbolRepr::from))
    }

    pub async fn update(
        pool: &SqlitePool,
        obj: impl AsRef<LocalSymbolRepr>,
    ) -> Result<(), SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "UPDATE local_symbol SET name = ? WHERE id = ?;",
            query_args![obj.name(), obj.id()],
        )
        .await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<LocalSymbolRepr>, SourcetrailError> {
        Ok(
            SqliteHelper::fetch::<LocalSymbol>(pool, "SELECT * FROM local_symbol;", query_args![])
                .await?
                .into_iter()
                .map(LocalSymbolRepr::from)
                .collect(),
        )
    }
}

#[derive(FromRow, Debug)]
struct SourceLocation {
    id: i64,
    file_node_id: i64,
    start_line: i32,
    start_column: i32,
    end_line: i32,
    end_column: i32,
    type_: i32,
}

impl TryFrom<SourceLocation> for SourceLocationRepr {
    type Error = SourcetrailError;

    fn try_from(loc: SourceLocation) -> Result<Self, Self::Error> {
        Self::new(
            loc.id,
            loc.file_node_id,
            loc.start_line,
            loc.start_column,
            loc.end_line,
            loc.end_column,
            loc.type_.try_into().map_err(SourcetrailError::convert)?,
        )
    }
}

pub struct SourceLocationDAO;

impl SourceLocationDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS source_location(id INTEGER PRIMARY KEY, file_node_id INTEGER, start_line INTEGER, start_column INTEGER, end_line INTEGER, end_column INTEGER, type INTEGER, FOREIGN KEY(file_node_id) REFERENCES node(id) ON DELETE CASCADE);", query_args![]).await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS source_location;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<SourceLocationRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(pool, "INSERT INTO source_location(id, file_node_id, start_line, start_column, end_line, end_column, type) VALUES(NULL, ?, ?, ?, ?, ?, ?);", query_args![obj.file_node_id(), obj.start_line(), obj.start_column(), obj.end_line(), obj.end_column(), obj.location_type() as i32]).await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "DELETE FROM source_location WHERE id = ?;",
            query_args![&id],
        )
        .await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM source_location;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(
        pool: &SqlitePool,
        id: i64,
    ) -> Result<Option<SourceLocationRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<SourceLocation>(
            pool,
            "SELECT * FROM source_location WHERE id = ?;",
            query_args![id],
        )
        .await?;

        result.map(SourceLocationRepr::try_from).transpose()
    }

    pub async fn update(
        pool: &SqlitePool,
        obj: impl AsRef<SourceLocationRepr>,
    ) -> Result<(), SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(pool, "UPDATE source_location SET file_node_id = ?, start_line = ?, start_column = ?, end_line = ?, end_column = ?, type = ? WHERE id = ?;", query_args![obj.file_node_id(), obj.start_line(), obj.start_column(), obj.end_line(), obj.end_column(), obj.location_type() as i32, obj.id()]).await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<SourceLocationRepr>, SourcetrailError> {
        SqliteHelper::fetch::<SourceLocation>(pool, "SELECT * FROM source_location;", query_args![])
            .await?
            .into_iter()
            .map(SourceLocationRepr::try_from)
            .collect::<Result<_, _>>()
    }
}

#[derive(FromRow, Debug)]
struct Occurrence {
    element_id: i64,
    source_location_id: i64,
}

impl From<Occurrence> for OccurrenceRepr {
    fn from(occ: Occurrence) -> Self {
        Self::new(occ.element_id, occ.source_location_id)
    }
}

pub struct OccurrenceDAO;

impl OccurrenceDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS occurrence(element_id INTEGER, source_location_id INTEGER, PRIMARY KEY(element_id, source_location_id), FOREIGN KEY(element_id) REFERENCES element(id) ON DELETE CASCADE, FOREIGN KEY(source_location_id) REFERENCES source_location(id) ON DELETE CASCADE);", query_args![]).await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS occurrence;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<OccurrenceRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "INSERT INTO occurrence(element_id, source_location_id) VALUES(?, ?);",
            query_args![obj.element_id(), obj.source_location_id()],
        )
        .await
    }

    pub async fn delete(pool: &SqlitePool, element_id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "DELETE FROM occurrence WHERE element_id = ?;",
            query_args![&element_id],
        )
        .await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM occurrence;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(
        pool: &SqlitePool,
        element_id: i64,
    ) -> Result<Option<OccurrenceRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<Occurrence>(
            pool,
            "SELECT * FROM occurrence WHERE element_id = ?;",
            query_args![&element_id],
        )
        .await?;

        Ok(result.map(OccurrenceRepr::from))
    }

    pub async fn update(
        pool: &SqlitePool,
        obj: impl AsRef<OccurrenceRepr>,
    ) -> Result<(), SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(
            pool,
            "UPDATE occurrence SET source_location_id = ? WHERE element_id = ?;",
            query_args![obj.source_location_id(), obj.element_id()],
        )
        .await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<OccurrenceRepr>, SourcetrailError> {
        Ok(
            SqliteHelper::fetch::<Occurrence>(pool, "SELECT * FROM occurrence;", query_args![])
                .await?
                .into_iter()
                .map(OccurrenceRepr::from)
                .collect(),
        )
    }
}

#[derive(FromRow, Debug)]
pub struct ComponentAccess {
    node_id: i64,
    type_: i32,
}

pub struct ComponentAccessDAO;

impl ComponentAccessDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS component_access(node_id INTEGER PRIMARY KEY, type INTEGER, FOREIGN KEY(node_id) REFERENCES node(id) ON DELETE CASCADE);", query_args![]).await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "DROP TABLE IF EXISTS component_access;",
            query_args![],
        )
        .await?;
        Ok(())
    }

    pub async fn new(pool: &SqlitePool, obj: &ComponentAccess) -> Result<i64, SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "INSERT INTO component_access(node_id, type) VALUES(?, ?);",
            query_args![&obj.node_id, &obj.type_],
        )
        .await
    }

    pub async fn delete(pool: &SqlitePool, node_id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "DELETE FROM component_access WHERE node_id = ?;",
            query_args![&node_id],
        )
        .await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM component_access;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(
        pool: &SqlitePool,
        node_id: i64,
    ) -> Result<Option<ComponentAccess>, SourcetrailError> {
        SqliteHelper::fetch_one(
            pool,
            "SELECT * FROM component_access WHERE node_id = ?;",
            query_args![&node_id],
        )
        .await
    }

    pub async fn update(pool: &SqlitePool, obj: &ComponentAccess) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "UPDATE component_access SET type = ? WHERE node_id = ?;",
            query_args![&obj.type_, &obj.node_id],
        )
        .await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<ComponentAccess>, SourcetrailError> {
        SqliteHelper::fetch(pool, "SELECT * FROM component_access;", query_args![]).await
    }
}

#[derive(FromRow, Debug)]
struct Error {
    id: i64,
    message: String,
    fatal: bool,
    indexed: bool,
    translation_unit: String,
}

impl From<Error> for ErrorRepr {
    fn from(err: Error) -> Self {
        Self::new(
            err.id,
            err.message,
            err.fatal,
            err.indexed,
            err.translation_unit,
        )
    }
}

pub struct ErrorDAO;

impl ErrorDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "CREATE TABLE IF NOT EXISTS error(id INTEGER PRIMARY KEY, message TEXT, fatal BOOLEAN, indexed BOOLEAN, translation_unit TEXT, FOREIGN KEY(id) REFERENCES element(id) ON DELETE CASCADE);", query_args![]).await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS error;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        obj: impl AsRef<ErrorRepr>,
    ) -> Result<i64, SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(pool, "INSERT INTO error(id, message, fatal, indexed, translation_unit) VALUES(?, ?, ?, ?, ?);", query_args![obj.id(), obj.message(), obj.is_fatal(), obj.is_indexed(), obj.translation_unit()]).await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM error WHERE id = ?;", query_args![&id]).await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM error;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<ErrorRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<Error>(
            pool,
            "SELECT * FROM error WHERE id = ?;",
            query_args![id],
        )
        .await?;

        Ok(result.map(ErrorRepr::from))
    }

    pub async fn update(
        pool: &SqlitePool,
        obj: impl AsRef<ErrorRepr>,
    ) -> Result<(), SourcetrailError> {
        let obj = obj.as_ref();
        SqliteHelper::exec(pool, "UPDATE error SET message = ?, fatal = ?, indexed = ?, translation_unit = ? WHERE id = ?;", query_args![obj.message(), obj.is_fatal(), obj.is_indexed(), obj.translation_unit(), obj.id()]).await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<ErrorRepr>, SourcetrailError> {
        Ok(
            SqliteHelper::fetch::<Error>(pool, "SELECT * FROM error;", query_args![])
                .await?
                .into_iter()
                .map(ErrorRepr::from)
                .collect(),
        )
    }
}

#[derive(Debug, FromRow)]
struct Meta {
    id: i64,
    key: String,
    value: String,
}

impl From<Meta> for MetaRepr {
    fn from(meta: Meta) -> Self {
        Self::new(meta.id, meta.key, meta.value)
    }
}

pub struct MetaDAO;

impl MetaDAO {
    pub async fn create_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "CREATE TABLE IF NOT EXISTS meta(id INTEGER PRIMARY KEY, key TEXT, value TEXT);",
            query_args![],
        )
        .await?;
        Ok(())
    }

    pub async fn delete_table(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DROP TABLE IF EXISTS meta;", query_args![]).await?;
        Ok(())
    }

    pub async fn new(
        pool: &SqlitePool,
        key: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<i64, SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "INSERT INTO meta(id, key, value) VALUES(NULL, ?, ?);",
            query_args![key.as_ref(), value.as_ref()],
        )
        .await
    }

    pub async fn delete(pool: &SqlitePool, id: i64) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM meta WHERE id = ?;", query_args![&id]).await?;
        Ok(())
    }

    pub async fn clear(pool: &SqlitePool) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(pool, "DELETE FROM meta;", query_args![]).await?;
        Ok(())
    }

    pub async fn get(pool: &SqlitePool, id: i64) -> Result<Option<MetaRepr>, SourcetrailError> {
        let result = SqliteHelper::fetch_one::<Meta>(
            pool,
            "SELECT * FROM meta WHERE id = ?;",
            query_args![&id],
        )
        .await?;

        Ok(result.map(MetaRepr::from))
    }

    pub async fn update(
        pool: &SqlitePool,
        id: i64,
        key: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<(), SourcetrailError> {
        SqliteHelper::exec(
            pool,
            "UPDATE meta SET key = ?, value = ? WHERE id = ?;",
            query_args![key.as_ref(), value.as_ref(), id],
        )
        .await?;
        Ok(())
    }

    pub async fn list(pool: &SqlitePool) -> Result<Vec<MetaRepr>, SourcetrailError> {
        Ok(
            SqliteHelper::fetch::<Meta>(pool, "SELECT * FROM meta;", query_args![])
                .await?
                .into_iter()
                .map(MetaRepr::from)
                .collect(),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[async_std::test]
    #[ignore]
    async fn test_db() -> Result<(), Box<dyn std::error::Error>> {
        let pool = SqliteHelper::connect("test.db").await?;

        ElementDAO::create_table(&pool).await?;
        let new_id = ElementDAO::new(&pool).await?;
        println!("Inserted new Element with ID: {}", new_id);

        let element = ElementDAO::get(&pool, new_id).await?;
        println!("Fetched Element: {:?}", element);

        ElementDAO::delete(&pool, new_id).await?;
        println!("Deleted Element with ID: {}", new_id);

        ElementDAO::clear(&pool).await?;
        println!("Cleared all Elements");

        ElementDAO::delete_table(&pool).await?;
        println!("Deleted element table");

        Ok(())
    }
}
