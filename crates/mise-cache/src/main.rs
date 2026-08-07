use clap::{Parser, Subcommand};
use eyre::{Context, Result, bail};
use mise_cache_core::{AGENT_PROTOCOL_VERSION, AgentRequest, AgentResponse, CacheDigest, LocalCas};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

#[derive(Debug, Parser)]
#[command(about = "Low-overhead build cache data plane for mise")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the per-user cache agent in the foreground.
    Agent {
        /// Local IPC endpoint (a Unix socket or Windows named pipe).
        #[arg(long)]
        socket: PathBuf,
        /// Directory containing the local content-addressable store.
        #[arg(long)]
        cache_dir: PathBuf,
    },
    /// Verify that an agent is reachable and protocol-compatible.
    Ping {
        #[arg(long)]
        socket: PathBuf,
    },
    /// Access the local CAS without starting an agent.
    Direct {
        #[arg(long)]
        cache_dir: PathBuf,
        #[command(subcommand)]
        command: DirectCommand,
    },
}

#[derive(Debug, Subcommand)]
enum DirectCommand {
    Find {
        algorithm: String,
        hash: String,
        size: u64,
    },
    Store {
        algorithm: String,
        hash: String,
        size: u64,
        source: PathBuf,
    },
}

#[derive(Clone)]
struct AgentState {
    cas: LocalCas,
    write_locks: Arc<Mutex<BTreeMap<CacheDigest, Weak<tokio::sync::Mutex<()>>>>>,
}

impl AgentState {
    fn new(cache_dir: PathBuf) -> Self {
        Self {
            cas: LocalCas::new(cache_dir),
            write_locks: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn write_lock(&self, digest: &CacheDigest) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.write_locks.lock().unwrap();
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(digest).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(tokio::sync::Mutex::new(()));
        locks.insert(digest.clone(), Arc::downgrade(&lock));
        lock
    }

    async fn respond(&self, request: AgentRequest) -> AgentResponse {
        let result = match request {
            AgentRequest::FindBlob { digest } => self
                .cas
                .find(&digest)
                .map(|path| AgentResponse::Blob { path }),
            AgentRequest::StoreBlob { digest, source } => {
                let lock = self.write_lock(&digest);
                let _guard = lock.lock().await;
                self.cas
                    .store_file(&digest, &source)
                    .map(|path| AgentResponse::Stored { path })
            }
            AgentRequest::Hello { .. } => {
                Err(eyre::eyre!("hello is only valid as the first request"))
            }
        };
        result.unwrap_or_else(|error| AgentResponse::Error {
            message: error.to_string(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Agent { socket, cache_dir } => run_agent(&socket, cache_dir).await,
        Command::Ping { socket } => ping(&socket).await,
        Command::Direct { cache_dir, command } => direct(cache_dir, command),
    }
}

fn direct(cache_dir: PathBuf, command: DirectCommand) -> Result<()> {
    let cas = LocalCas::new(cache_dir);
    match command {
        DirectCommand::Find {
            algorithm,
            hash,
            size,
        } => {
            let digest = CacheDigest {
                algorithm,
                hash,
                size,
            };
            if let Some(path) = cas.find(&digest)? {
                println!("{}", path.display());
            }
        }
        DirectCommand::Store {
            algorithm,
            hash,
            size,
            source,
        } => {
            let digest = CacheDigest {
                algorithm,
                hash,
                size,
            };
            println!("{}", cas.store_file(&digest, &source)?.display());
        }
    }
    Ok(())
}

async fn handle_connection<S>(stream: S, state: AgentState) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut lines = BufReader::new(reader).lines();
    let hello = lines
        .next_line()
        .await?
        .ok_or_else(|| eyre::eyre!("connection closed before the agent handshake"))?;
    let request: AgentRequest = serde_json::from_str(&hello)?;
    match request {
        AgentRequest::Hello { protocol, .. } if protocol == AGENT_PROTOCOL_VERSION => {}
        AgentRequest::Hello { protocol, .. } => {
            send_response(
                &mut writer,
                &AgentResponse::Error {
                    message: format!(
                        "unsupported agent protocol {protocol}; expected {AGENT_PROTOCOL_VERSION}"
                    ),
                },
            )
            .await?;
            return Ok(());
        }
        _ => bail!("the first agent request must be hello"),
    }
    send_response(
        &mut writer,
        &AgentResponse::Hello {
            protocol: AGENT_PROTOCOL_VERSION,
            agent_version: env!("CARGO_PKG_VERSION").to_string(),
        },
    )
    .await?;

    while let Some(line) = lines.next_line().await? {
        let response = match serde_json::from_str(&line) {
            Ok(request) => state.respond(request).await,
            Err(error) => AgentResponse::Error {
                message: format!("invalid agent request: {error}"),
            },
        };
        send_response(&mut writer, &response).await?;
    }
    Ok(())
}

async fn send_response(
    writer: &mut (impl AsyncWrite + Unpin),
    response: &AgentResponse,
) -> Result<()> {
    let mut encoded = serde_json::to_vec(response)?;
    encoded.push(b'\n');
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

async fn handshake<S>(stream: &mut S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = AgentRequest::Hello {
        protocol: AGENT_PROTOCOL_VERSION,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let mut encoded = serde_json::to_vec(&request)?;
    encoded.push(b'\n');
    stream.write_all(&encoded).await?;
    stream.flush().await?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).await?;
    match serde_json::from_str(&response)? {
        AgentResponse::Hello { protocol, .. } if protocol == AGENT_PROTOCOL_VERSION => Ok(()),
        AgentResponse::Error { message } => bail!(message),
        _ => bail!("agent returned an invalid handshake response"),
    }
}

#[cfg(unix)]
async fn ping(socket: &Path) -> Result<()> {
    let mut stream = tokio::net::UnixStream::connect(socket)
        .await
        .wrap_err_with(|| format!("failed to connect to cache agent at {}", socket.display()))?;
    handshake(&mut stream).await?;
    println!("ok");
    Ok(())
}

#[cfg(windows)]
async fn ping(socket: &Path) -> Result<()> {
    let name = socket.to_string_lossy();
    let mut stream = tokio::net::windows::named_pipe::ClientOptions::new()
        .open(name.as_ref())
        .wrap_err_with(|| format!("failed to connect to cache agent at {name}"))?;
    handshake(&mut stream).await?;
    println!("ok");
    Ok(())
}

#[cfg(unix)]
async fn run_agent(socket: &Path, cache_dir: PathBuf) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    if let Some(parent) = socket.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if socket.exists() {
        match tokio::net::UnixStream::connect(socket).await {
            Ok(_) => bail!("a cache agent is already listening at {}", socket.display()),
            Err(_) => {
                std::fs::remove_file(socket).wrap_err("failed to remove stale agent socket")?
            }
        }
    }
    let listener = tokio::net::UnixListener::bind(socket)?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    let _cleanup = SocketCleanup(socket.to_path_buf());
    let state = AgentState::new(cache_dir);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, state).await {
                        eprintln!("mise-cache agent connection failed: {error}");
                    }
                });
            }
            result = tokio::signal::ctrl_c() => {
                result?;
                return Ok(());
            }
        }
    }
}

#[cfg(unix)]
struct SocketCleanup(PathBuf);

#[cfg(unix)]
impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(windows)]
async fn run_agent(socket: &Path, cache_dir: PathBuf) -> Result<()> {
    use tokio::net::windows::named_pipe::ServerOptions;

    let name = socket.to_string_lossy().into_owned();
    let state = AgentState::new(cache_dir);
    let mut first = true;
    loop {
        let mut options = ServerOptions::new();
        if first {
            options.first_pipe_instance(true);
            first = false;
        }
        let server = options.create(&name)?;
        tokio::select! {
            result = server.connect() => {
                result?;
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(server, state).await {
                        eprintln!("mise-cache agent connection failed: {error}");
                    }
                });
            }
            result = tokio::signal::ctrl_c() => {
                result?;
                return Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn agent_handshake_and_blob_round_trip() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        std::fs::write(&source, b"cached object").unwrap();
        let digest = CacheDigest::blake3(b"cached object");
        let state = AgentState::new(directory.path().join("cache"));
        let (mut client, server) = tokio::io::duplex(16 * 1024);
        let task = tokio::spawn(handle_connection(server, state));

        handshake(&mut client).await.unwrap();
        let request = AgentRequest::StoreBlob { digest, source };
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        client.write_all(&encoded).await.unwrap();
        let mut response = String::new();
        BufReader::new(&mut client)
            .read_line(&mut response)
            .await
            .unwrap();
        assert!(matches!(
            serde_json::from_str(&response).unwrap(),
            AgentResponse::Stored { .. }
        ));
        drop(client);
        task.await.unwrap().unwrap();
    }
}
