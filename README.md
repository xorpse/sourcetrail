# sourcetrail

Build and manipulate [Sourcetrail](https://github.com/CoatiSoftware/Sourcetrail) databases. API (heavily) inspired by [numbat](https://github.com/quarkslab/numbat).

## Example (from numbat)

```rust
use sourcetrail::SourcetrailDB;

let mut db = SourcetrailDB::create("test").await?;

let my_main = db
    .record_class()
    .name("MyMainClass")
    .commit()
    .await?;

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
```
