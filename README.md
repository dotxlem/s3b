# s3b

s3b (pronounced "seb") is a command line tool for uploading data to Amazon S3, backed by an embedded database ([GlueSQL](https://gluesql.org/docs)).

The s3b workflow is inspired in part by the [Terraform](https://terraform.io) CLI. The main commands are `plan`, which builds a changeset of files which will be uploaded, and `push` (analogous to `terraform apply`) which will execute the plan.

The embedded database is stored as JSON in the target bucket, and is used to track the BLAKE3 hash, origin path, and modified timestamp of each object. This database is queried to determine whether an object can be skipped before uploading, and can also be queried to determine (among other things) if duplicate objects are stored at multiple keys.

The bucket key of uploaded files are relative to the directory from which the s3b plan is generated.

## Commands

### plan

### push

### info

### find

## Examples