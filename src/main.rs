use anyhow::Result;
use clap::{crate_authors, crate_version, AppSettings, Clap};
use d7sneakers::{Constraints, SneakerWorld};
extern crate pretty_env_logger;
#[macro_use]
extern crate log;

#[derive(Clap)]
#[clap(version = crate_version!(), author = crate_authors!())]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    /// Sets a custom config file.
    //#[clap(short, long, default_value = "default.conf")]
    //config: String,
    /// Sets base directory.
    #[clap(short, long, default_value = "/tmp/d7sneaker")]
    basedir: String,
    /// A level of verbosity, and can be used multiple times
    #[clap(short, long, parse(from_occurrences))]
    verbose: i32,
    #[clap(subcommand)]
    subcmds: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    //#[clap(version = "1.3", author = "Someone E. <someone_else@other.com>")]
    Add(Add),
    Sys(Sys),
    Query(Query),
}
/// Add bundles in various forms
#[derive(Clap)]
struct Add {
    /// Add bundle provided as hex string
    #[clap(short, long)]
    hex: Option<String>,
    /// Add bundles from a directory
    #[clap(short, long)]
    path: Option<String>,
    /// Add bundles recursively (in combination with --path)
    #[clap(short, long)]
    recursive: bool,
}

/// Perform various maintenance tasks on the system
#[derive(Clap)]
struct Sys {
    /// reparse filesystem
    #[clap(short, long)]
    fs: bool,
    /// cleanup database
    #[clap(short, long)]
    db: bool,
}

/// Query the database
#[derive(Clap)]
struct Query {
    /// list all bundle ids
    #[clap(short, long)]
    ids: bool,
    /// print infos for bundle id
    #[clap(short, long)]
    print_infos: Option<String>,
    /// print all bundle IDs with constraint ForwardPending
    #[clap(short, long)]
    forward: bool,
    /// print all bundle IDs with constraint DispatchPending
    #[clap(short, long)]
    dispatch: bool,
    /// print all bundle IDs with constraint ReassemblyPending
    #[clap(short, long)]
    /// print all bundle IDs with constraint Contraindicated
    reassembly: bool,
    #[clap(short, long)]
    contra: bool,
    /// print all bundle IDs with constraint LocalEndpoint
    #[clap(short, long)]
    local: bool,
    /// print all bundle IDs with constraints
    #[clap(short, long)]
    all_constraints: bool,
    /// list all bundle IDs with either src or dst matching node query
    #[clap(short, long)]
    query_node: Option<String>,
}
fn main() -> Result<()> {
    let opts: Opts = Opts::parse();

    // Vary the output based on how many times the user used the "verbose" flag
    // (i.e. 'myprog -v -v -v' or 'myprog -vvv' vs 'myprog -v'
    match opts.verbose {
        0 => std::env::set_var("RUST_LOG", ""),
        1 => std::env::set_var("RUST_LOG", "d7sneakers=info"),
        2 | _ => std::env::set_var("RUST_LOG", "d7sneakers=debug"),
    }
    pretty_env_logger::init_timed();

    // Gets a value for config if supplied by user, or defaults to "default.conf"
    // debug!("Value for config: {}", opts.config);
    debug!("Value for basedir: {}", opts.basedir);

    let sneakers = SneakerWorld::new(&opts.basedir);
    sneakers.fs.setup()?;

    match opts.subcmds {
        SubCommand::Add(a) => {
            if let Some(input) = a.hex {
                let bndl = sneakers.fs.import_hex(&input)?;
                sneakers.db.insert(&bndl)?;
            } else if let Some(path) = a.path {
                sneakers.import_dir(&path, a.recursive)?;
            }
        }
        SubCommand::Sys(m) => {
            if m.db && m.fs {
                sneakers.sync()?;
            } else if m.db {
                sneakers.db.sync_with_fs(&sneakers.fs)?;
            } else if m.fs {
                sneakers.fs.sync_to_db(&sneakers.db)?;
            }
        }
        SubCommand::Query(q) => {
            if q.ids {
                println!("{:#?}", sneakers.db.ids());
            } else if q.print_infos.is_some() {
                let bid = q.print_infos.unwrap();
                println!("{:#?}", sneakers.db.get_bundle_entry(&bid).unwrap());
                println!("{:#?}", sneakers.db.get_constraints(&bid).unwrap());
            } else if q.all_constraints {
                println!("{:#?}", sneakers.db.all_constraints());
            } else if q.forward {
                println!(
                    "{:#?}",
                    sneakers.db.filter_constraints(Constraints::FORWARD_PENDING)
                );
            } else if q.dispatch {
                println!(
                    "{:#?}",
                    sneakers
                        .db
                        .filter_constraints(Constraints::DISPATCH_PENDING)
                );
            } else if q.reassembly {
                println!(
                    "{:#?}",
                    sneakers
                        .db
                        .filter_constraints(Constraints::REASSEMBLY_PENDING)
                );
            } else if q.contra {
                println!(
                    "{:#?}",
                    sneakers.db.filter_constraints(Constraints::CONTRAINDICATED)
                );
            } else if q.local {
                println!(
                    "{:#?}",
                    sneakers.db.filter_constraints(Constraints::LOCAL_ENDPOINT)
                );
            } else if let Some(node) = q.query_node {
                println!("{:#?}", sneakers.db.filter_node(&node));
            }
        }
    }

    Ok(())
}