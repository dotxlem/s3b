use clap::{arg, command};

fn main() {
    let matches = command!()
        .arg(arg!([input]).required(true))
        .get_matches();

    println!("{:?}", matches.get_one::<String>("input"));

    // init
    //  Check bucket for DB file; if not present, create & upload 

    // up
    //  Get path
    //  Walkdir from path & enumerate files to upload
    //  Fetch DB file and lock bucket
    //  Check uplist against DB for duplicates
    //  Iterate over uplist and hash with blake3 as upped; write to local DB file as upped.
    //  Upload DB file after list is complete
}
