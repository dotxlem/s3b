# s3b

s3b (pronounced "seb") is a command line tool for uploading data to Amazon S3, backed by an embedded database ([GlueSQL](https://gluesql.org/docs)).

The s3b workflow is inspired in part by the [Terraform](https://terraform.io) CLI. The main commands are `plan`, which builds a changeset of files which will be uploaded, and `push` (analogous to `terraform apply`) which will execute the plan.

The embedded database is stored as JSON in the target bucket, and is used to track the BLAKE3 hash, origin path, and modified timestamp of each object. This database is queried to determine whether an object can be skipped before uploading, and can also be queried to determine (among other things) if duplicate objects are stored at multiple keys.

The bucket key of uploaded files are relative to the directory from which the s3b plan is generated.

## Commands

### plan
`s3b plan --bucket <BUCKET> --include <LIST> --exclude <LIST>` 

Generates a plan file against the specified bucket for files in the current directory.

Arguments:  
`bucket` [REQUIRED]: the name of an existing S3 bucket  
`include` [OPTIONAL]: a space-separated list of path filters to include in the plan  
`exclude` [OPTIONAL]: a space-separated list of path filters to exclude from the plan  

Notes:  
Include & exclude filters match if the filter string is found in the path. For example passing `--exclude .git` will exclude any file paths containing `.git`. To narrow the filter, `--exclude path/to/project/.git` would exclude files in a specific .git directory.

Examples:  
- Include `Projects/` directory and exclude common build & artifact directories  
  `s3b plan --bucket my-bucket --include Projects --exclude target build node_modules`
- Suppose a directory named `Go/` exists in the current directory and in the `Projects/` directory  
  `s3b plan --bucket my-bucket --include Go` will include both `Go/` and `Projects/Go/`  
  `s3b plan --bucket my-bucket --include Go --exclude Projects/Go` will include only `Go/`  
  `s3b plan --bucket my-bucket --include Projects/Go` will include only `Projects/Go`  

### push
`s3b push` 

Push takes no arguments; if there is an `s3b_plan.bin` in the current directory it will execute the plan and push any listed files to the bucket specified in the plan.

### info
`s3b info --bucket <BUCKET> --key <KEY>` 

Print information such as hash and origin path for the given key. 

Arguments:  
`bucket` [REQUIRED]: the name of an existing S3 bucket  
`key` [REQUIRED]: the name of an existing object in the bucket

### find
`s3b find --bucket <BUCKET> --where <WHERE CLAUSE>` 

Run an SQL SELECT query against the embedded database in the given bucket, using the specified WHERE clause.

Arguments:  
`bucket` [REQUIRED]: the name of an existing S3 bucket  
`where` [REQUIRED]: the WHERE clause to pass to the SELECT query; should be in double-quotes  

Examples: 
- Find all uploaded objects where the origin path starts with `/home/xlem`:  
  `s3b find --bucket my-bucket --where "path LIKE '/home/xlem%'"`
- Find all uploaded object where the hash is "06556521595c9d9f8a5865de2a37c2a3f5d89481c20213dfd24c120c7e84a4cb":  
  `s3b find --bucket my-bucket --where "hash='06556521595c9d9f8a5865de2a37c2a3f5d89481c20213dfd24c120c7e84a4cb'"` 

Notes:  
Column names are `key`, `hash`, `path`, and `modified`. All are TEXT except modified which is UINT64.
For help, see the [GlueSQL WHERE clause docs](https://gluesql.org/docs/0.16.0/sql-syntax/statements/querying/where).